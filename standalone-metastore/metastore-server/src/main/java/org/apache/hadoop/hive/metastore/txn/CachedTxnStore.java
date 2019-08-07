package org.apache.hadoop.hive.metastore.txn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.InitializeTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ReplTblWriteIdStateRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnInfo;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.TxnState;
import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.AcidWriteEvent;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class CachedTxnStore implements TxnStore {
  static final private Logger LOG = LoggerFactory.getLogger(CachedTxnStore.class.getName());

  // TxnHandler to delegate the task which remains DB focused
  private CompactionTxnHandler dbTxnHandler;

  private Lock txnLock = new ReentrantLock(true);
  // CREATE TABLE TXNS + TXN_COMPONENTS (List should be ordered by txnId ascending)
  private LinkedList<TxnTable> openOrAbortedTxns;
  // TxnId -> TxnTable
  private Map<Long, TxnTable> openOrAbortedTxnsById;
  // TxnId -> TxnTable for committed transactions above minOpenTransaction with write
  private Map<Long, TxnTable> committedWriteTxnsById;
  // CREATE TABLE TXN_COMPONENTS + COMPLETED_TXN_COMPONENTS
  // "Db.Table" -> TxnComponent (List should be ordered by writeId ascending)
  // Open and aborted writes, and committed writes for transactions
  // (committed writes are only above minOpenTransaction)
  private Map<String, LinkedList<TxnComponentsTable>> recentWrites;
  // "Db.Table" -> TxnComponent
  private Map<String, TxnComponentsTable> lastCompletedWritesMap;
  // CREATE TABLE NEXT_TXN_ID
  private long nextTxnId = 1;
  // CREATE TABLE HIVE_LOCKS
  // TxnId -> List<Lock>
  private Map<Long, Set<ShowLocksResponseElement>> locksByTnxId;
  // "Db.Table" -> Partition ("" for table level) -> Lock
  private Map<String, Map<String, ShowLocksResponseElement>> locks;
  // CREATE TABLE NEXT_LOCK_ID
  private long nextLockId = 1;
  // COMPACTION_QUEUE
  private List<CompactionInfo> pendingCompactions;
  // CREATE TABLE NEXT_COMPACTION_QUEUE_ID
  private long nextCompactionId = 1;
  // CREATE TABLE COMPLETED_COMPACTIONS
  private Set<CompactionInfo> completedCompactions;
  // CREATE TABLE NEXT_WRITE_ID
  // "Db.Table" -> NextWriteId
  private Map<String, Long> newWriteId;
  // CREATE TABLE MIN_HISTORY_LEVEL
  // TxnId -> MinTxnId
  private Map<Long, Long> minOpenTxnIdWhenOpened;
  private long minOpenTxnId;
  // CREATE TABLE MATERIALIZATION_REBUILD_LOCKS
  // TxnId -> Lock
  private Map<Long, MaterializationRebuildLock> materializationRebuildLocks;
  // CREATE TABLE REPL_TXN_MAP
  // Policy->SourceTxnId->TargetTxnId
  private Map<String, Map<Long, Long>> replTxnMap;

//  private LinkedList<TxnInfo> openAndAbortedTxnInfos;
//  private Map<Long, Long> minOpenTxnIdWhenOpened;
//  private Map<Long, TxnChangeComponent> txnComponents;
//  // 'DatabaseName.TableName' -> NextWriteId
//  private Map<String, Long> nextWriteIdForTable;
//  // Current maximum txnId
//  private long highWaterMark = 0;
//
//  // Calculated
//  // TxnId -> TxnInfo
//  private Map<Long, TxnInfo> txnInfoMap;
//  // WriteId -> TxnInfo
//  //private Map<Long, TxnInfo>

  // Current open transactions
  private long numOpenTxns = 0;

  // Maximum number of open transactions that's allowed
  private static volatile int maxOpenTxns = 0;
  // Maximum size of batch for open transactions that's allowed
  private static volatile int maxOpenTxnsBatchSize = 0;
  // Whether number of open transactions reaches the threshold
  private static volatile boolean tooManyOpenTxns = false;

  public CachedTxnStore() {
    dbTxnHandler = new CompactionTxnHandler();
  }

  private List<TxnInfo> getTxnInfos(List<TxnTable> source) {
    return source.stream()
        .map(txnInfo -> txnInfo.toTxnInfo())
        .collect(Collectors.toList());
  }

  @Override
  public GetOpenTxnsInfoResponse getOpenTxnsInfo() throws MetaException {
    txnLock.lock();
    try {
      return new GetOpenTxnsInfoResponse(nextTxnId - 1, getTxnInfos(openOrAbortedTxns));
    } finally {
      txnLock.unlock();
    }
  }

  @Override
  public GetOpenTxnsResponse getOpenTxns() throws MetaException {
    txnLock.lock();
    try {
      List<Long> result = new ArrayList<>(openOrAbortedTxns.size());
      BitSet abortedBits = new BitSet();
      for(TxnTable txn : openOrAbortedTxns) {
        result.add(txn.id);
        if (txn.state == TxnState.ABORTED) {
          abortedBits.set(result.size() - 1);
        }
      }

      ByteBuffer byteBuffer = ByteBuffer.wrap(abortedBits.toByteArray());
      return new GetOpenTxnsResponse(nextTxnId - 1, result, byteBuffer);
    } finally {
      txnLock.unlock();
    }
  }

  @Override
  public void countOpenTxns() throws MetaException {
    numOpenTxns = openOrAbortedTxns.stream()
                      .filter(txn -> txn.state == TxnState.OPEN)
                      .count();
  }

  private void checkOpenTxnNumber(int newTransactions) throws MetaException {
    if (!tooManyOpenTxns && numOpenTxns + newTransactions >= maxOpenTxns) {
      tooManyOpenTxns = true;
    }
    if (tooManyOpenTxns) {
      if (numOpenTxns + newTransactions < maxOpenTxns * 0.9) {
        tooManyOpenTxns = false;
      } else {
        LOG.warn("Maximum allowed number of open transactions (" + maxOpenTxns + ") has been " +
             "reached. Current number of open transactions: " + numOpenTxns + ". Requested " +
             newTransactions + " more.");
        throw new MetaException("Maximum allowed number of open transactions has been reached. " +
                                    "See hive.max.open.txns.");
      }
    }
  }

  private List<Long> getTargetTxnIdList(String replPolicy, List<Long> sourceTxnIds) {
    Map<Long, Long> txnMap = replTxnMap.get(replPolicy);
    return sourceTxnIds.stream()
               .map(txnId -> txnMap.get(txnId))
               .collect(Collectors.toList());
  }

  private List<Long> openTxns(int numTxns, String user, String host, String replPolicy, List<Long> replSrcTxnIds)
      throws MetaException {
    long currentTime = System.currentTimeMillis();
    TxnType txnType = TxnType.DEFAULT;
    Map<Long, Long> txnMap = null;

    txnLock.lock();
    try {
      if (replPolicy != null) {
        // If replication, check if we already generated txnId for these transactions
        List<Long> targetTxnIdList = getTargetTxnIdList(replPolicy, replSrcTxnIds);

        if (!targetTxnIdList.isEmpty()) {
          if (targetTxnIdList.size() != replSrcTxnIds.size()) {
            LOG.warn("target txn id number " + targetTxnIdList.toString() +
                         " is not matching with source txn id number " + replSrcTxnIds.toString());
          }
          LOG.info("Target transactions " + targetTxnIdList.toString() + " are present for repl policy :" +
                       replPolicy + " and Source transaction id : " + replSrcTxnIds.toString());
          return targetTxnIdList;
        }
        txnType = TxnType.REPL_CREATED;
        txnMap = replTxnMap.get(replPolicy);
        if (txnMap == null) {
          txnMap = new HashMap<Long, Long>();
          replTxnMap.put(replPolicy, txnMap);
        }
      }
      // Get the current lowest open transaction id
      Long currentMinOpenTxnId = openOrAbortedTxns.stream()
                              .filter(txn -> txn.state == TxnState.OPEN)
                              .findFirst()
                              .get().id;

      List<Long> newTxnIds = new ArrayList<>(numTxns);
      for(int i = 0; i < numTxns; i++) {
        TxnTable txn = new TxnTable(nextTxnId++, user, host, txnType, currentTime);
        openOrAbortedTxns.add(txn);
        openOrAbortedTxnsById.put(txn.id, txn);
        if (minOpenTxnId == Long.MAX_VALUE) {
          minOpenTxnId = currentMinOpenTxnId;
        }
        minOpenTxnIdWhenOpened.put(txn.id, currentMinOpenTxnId);
        numOpenTxns++;
        newTxnIds.add(txn.id);
        if (txnMap != null) {
          txnMap.put(replSrcTxnIds.get(i), txn.id);
        }
      }
      return newTxnIds;
    } finally {
      txnLock.unlock();
    }

  }

  @Override
  public OpenTxnsResponse openTxns(OpenTxnRequest rqst) throws MetaException {
    int numTxns = rqst.getNum_txns();
    if (numTxns <= 0) {
      throw new MetaException("Invalid input for number of txns: " + numTxns);
    }
    checkOpenTxnNumber(numTxns);
    if (numTxns > maxOpenTxnsBatchSize) numTxns = maxOpenTxnsBatchSize;

    List<Long> newTxnIds = openTxns(numTxns, rqst.getUser(), rqst.getHostname(), rqst.getReplPolicy(), rqst.getReplSrcTxnIds());

    return new OpenTxnsResponse(newTxnIds);
  }

  @Override
  public long getTargetTxnId(String replPolicy, long sourceTxnId) throws MetaException {
    txnLock.lock();
    try {
      return replTxnMap.get(replPolicy).get(sourceTxnId);
    } finally {
      txnLock.unlock();
    }
  }

//  /**
//   * Used to raise an informative error when the caller expected a txn in a particular TxnStatus
//   * but found it in some other status
//   */
//  private static void raiseTxnUnexpectedState(TxnHandler.TxnStatus actualStatus, long txnid)
//      throws NoSuchTxnException, TxnAbortedException {
//    switch (actualStatus) {
//      case ABORTED:
//        throw new TxnAbortedException("Transaction " + JavaUtils.txnIdToString(txnid) + " already aborted");
//      case COMMITTED:
//        throw new NoSuchTxnException("Transaction " + JavaUtils.txnIdToString(txnid) + " is already committed.");
//      case OPEN:
//        throw new NoSuchTxnException(JavaUtils.txnIdToString(txnid) + " is " + TxnHandler.TxnStatus.OPEN);
//      default:
//        throw new IllegalArgumentException("Unknown TxnStatus " + actualStatus);
//    }
//  }

  private void deleteReplTxnMapEntry(long sourceTxnId, String replPolicy) {
    Map<Long, Long> txnMap = replTxnMap.get(replPolicy);
    if (txnMap != null) {
      txnMap.remove(sourceTxnId);
      if (txnMap.isEmpty()) {
        replTxnMap.remove(replPolicy);
      }
    }
  }

  private void updateMinOpenedTxn(long removedTxnId) {
    minOpenTxnIdWhenOpened.remove(removedTxnId);
    // Only it this causes a change in minOpenTxnId
    if (removedTxnId == minOpenTxnId) {
      // Update minOpenTxnId
      if (!minOpenTxnIdWhenOpened.isEmpty()) {
        minOpenTxnId = minOpenTxnIdWhenOpened.values().stream().mapToLong(i -> i).min().getAsLong();
      } else {
        minOpenTxnId = Long.MAX_VALUE;
      }
      // Clean up committedWriteTxnsById from not needed values
      Iterator<TxnTable> committedTxns = committedWriteTxnsById.values().iterator();
      while (committedTxns.hasNext()) {
        TxnTable committedTxn = committedTxns.next();
        if (committedTxn.id < minOpenTxnId) {
          committedTxns.remove();
          committedTxn.components.forEach(write -> removeRecentWrite(write));
        }
      }
    }
  }

  @Override
  public void abortTxn(AbortTxnRequest rqst) throws NoSuchTxnException, MetaException, TxnAbortedException {
    long txnid = rqst.getTxnid();
    long sourceTxnId = -1;

    txnLock.lock();
    try {
      if (rqst.isSetReplPolicy()) {
        sourceTxnId = rqst.getTxnid();
        List<Long> targetTxnIds = getTargetTxnIdList(rqst.getReplPolicy(),
            Collections.singletonList(sourceTxnId));
        if (targetTxnIds.isEmpty()) {
          // Idempotent case where txn was already closed or abort txn event received without
          // corresponding open txn event.
          LOG.info("Target txn id is missing for source txn id : " + sourceTxnId +
                       " and repl policy " + rqst.getReplPolicy());
          return;
        }
        txnid = targetTxnIds.get(0);
      }
      if (abortTxns(Collections.singletonList(txnid), true) != 1) {
        TxnHandler.TxnStatus actualTxnStatus = findTxnState(txnid);
        if(actualTxnStatus == TxnHandler.TxnStatus.ABORTED) {
          if (rqst.isSetReplPolicy()) {
            // in case of replication, idempotent is taken care by getTargetTxnId
            LOG.warn("Invalid state ABORTED for transactions started using replication replay task");
            deleteReplTxnMapEntry(sourceTxnId, rqst.getReplPolicy());
          }
          LOG.info("abortTxn(" + JavaUtils.txnIdToString(txnid) +
                       ") requested by it is already " + TxnHandler.TxnStatus.ABORTED);
          return;
        }
        TxnHandler.raiseTxnUnexpectedState(actualTxnStatus, txnid);
      }

      if (rqst.isSetReplPolicy()) {
        deleteReplTxnMapEntry(sourceTxnId, rqst.getReplPolicy());
      }
      // TODO TransactionalListener
    } finally {
      txnLock.unlock();
    }
  }

  @Override
  public void abortTxns(AbortTxnsRequest rqst) throws NoSuchTxnException, MetaException {
    int numAborted = abortTxns(rqst.getTxn_ids(), false);
    if (numAborted != rqst.getTxn_ids().size()) {
      LOG.warn("Abort Transactions command only aborted " + numAborted + " out of " +
                   rqst.getTxn_ids().size() + " transactions. It's possible that the other " +
                   (rqst.getTxn_ids().size() - numAborted) +
                   " transactions have been aborted or committed, or the transaction ids are invalid.");
    }
    // TODO TransactionalListener
  }

  private void releaseLocks(Set<ShowLocksResponseElement> locks) {
    locks.stream().forEach(lock -> {
      locksByTnxId.get(lock.getTxnid()).remove(lock);
      locks.remove(lock);
    });
  }

  private int abortTxns(List<Long> txnIds, boolean failOnError) throws MetaException {
    int abortedNum = 0;
    txnLock.lock();
    try {
      for(Long txnId : txnIds) {
        TxnTable txn = openOrAbortedTxnsById.get(txnId);
        if (txn != null) {
          txn.state = TxnState.ABORTED;
          updateMinOpenedTxn(txnId);
          releaseLocks(locksByTnxId.get(txnId));
          locksByTnxId.remove(txnId);
          numOpenTxns--;
          abortedNum++;
        } else {
          if (failOnError) {
            throw new MetaException("Not all the transactions are aborted, since txnId=" + txnId + " unknown");
          }
        }
      }
    } finally {
      txnLock.unlock();
    }
    return abortedNum;
  }

  /**
   * Returns the state of the transaction if it's able to determine it. Some cases where it cannot:
   * 1. txnId was Aborted/Committed and then GC'd (compacted)
   * 2. txnId was committed but it didn't modify anything (nothing in COMPLETED_TXN_COMPONENTS)
   */
  private TxnHandler.TxnStatus findTxnState(long txnId) throws MetaException {
    TxnTable txn = openOrAbortedTxnsById.get(txnId);
    if (txn == null) {
      txn = committedWriteTxnsById.get(txnId);
    }
    if (txn != null) {
      switch (txn.state) {
        case ABORTED:
          return TxnHandler.TxnStatus.ABORTED;
        case OPEN:
          return TxnHandler.TxnStatus.OPEN;
        case COMMITTED:
          return TxnHandler.TxnStatus.COMMITTED;
      }
    }
    return TxnHandler.TxnStatus.UNKNOWN;
  }

  private String generateAbortMessage(TxnComponentsTable conflictingComponent, TxnComponentsTable committedComponent) {
    StringBuilder msg = new StringBuilder();
    msg.append("Aborting [")
        .append(JavaUtils.txnIdToString(conflictingComponent.txnId)).append(",")
        .append(conflictingComponent.txnIdWhenCommitted)
        .append("] due to a write conflict on ")
        .append(conflictingComponent.database).append("/")
        .append(conflictingComponent.table);
    if (!conflictingComponent.partition.isEmpty()) {
      msg.append("/").append(conflictingComponent.partition);
    }
    msg.append(" committed by [")
        .append(JavaUtils.txnIdToString(committedComponent.writeId)).append(",")
        .append(committedComponent.txnIdWhenCommitted).append("] ")
        .append(conflictingComponent.type).append("/").append(committedComponent.type);
    return msg.toString();
  }

  private TxnComponentsTable getLastCompletedWrite(String database, String table) {
    return lastCompletedWritesMap.get(database + "." + table);
  }

  private void updateLastCompletedWrite(TxnComponentsTable write) {
    String fullTableName = write.database + "." + write.table;
    TxnComponentsTable lastWrite = lastCompletedWritesMap.get(fullTableName);
    if (lastWrite == null || lastWrite.writeId < write.writeId) {
      lastCompletedWritesMap.put(fullTableName, write);
    }
  }

  private void removeRecentWrite(TxnComponentsTable component) {
    String fullTableName = component.database + "." + component.table;
    List<TxnComponentsTable> writeList = recentWrites.get(fullTableName);
    if (writeList != null) {
      writeList.remove(component);
    }
    if (writeList.size() == 0) {
      recentWrites.remove(fullTableName);
    }
  }

  private void addRecentWrite(TxnComponentsTable write) {
    String fullTableName = write.database + "." + write.table;
    List<TxnComponentsTable> writeList = recentWrites.get(fullTableName);
    if (writeList == null) {
      writeList = new LinkedList<>();
    }
    writeList.add(write);
  }

  private static LinkedList<TxnComponentsTable> EMPTY_LINKED_LIST = new LinkedList<>();
  private LinkedList<TxnComponentsTable> getRecentWriteList(String database, String table) {
    String fullTableName = database + "." + table;
    LinkedList<TxnComponentsTable> writeList = recentWrites.get(fullTableName);
    if (writeList == null) {
      return EMPTY_LINKED_LIST;
    } else {
      return writeList;
    }
  }

  private void removeLocks(Set<ShowLocksResponseElement> locksToRemove) {
    locksToRemove.forEach(lock -> {
      Map<String, ShowLocksResponseElement> lockMap = locks.get(lock.getDbname() + "." + lock.getTablename());
      lockMap.remove(lock.getPartname());
      if (lockMap.isEmpty()) {
        locks.remove(lock.getDbname() + "." + lock.getTablename());
      }
    });
  }

  @Override
  public void commitTxn(CommitTxnRequest rqst) throws NoSuchTxnException, TxnAbortedException, MetaException {
    long txnId = rqst.getTxnid();
    long sourceTxnId = -1;
    if (rqst.isSetKeyValue()) {
      if (!rqst.getKeyValue().getKey().startsWith(TxnStore.TXN_KEY_START)) {
        String errorMsg = "Error updating key/value in the sql backend with"
                              + " txnId=" + rqst.getTxnid() + ","
                              + " tableId=" + rqst.getKeyValue().getTableId() + ","
                              + " key=" + rqst.getKeyValue().getKey() + ","
                              + " value=" + rqst.getKeyValue().getValue() + "."
                              + " key should start with " + TXN_KEY_START + ".";
        LOG.warn(errorMsg);
        throw new IllegalArgumentException(errorMsg);
      }
    }
    txnLock.lock();
    try {
      // TODO Update replication last id
      if (rqst.isSetReplPolicy()) {
        sourceTxnId = rqst.getTxnid();
        List<Long> targetTxnIds = getTargetTxnIdList(rqst.getReplPolicy(),
            Collections.singletonList(sourceTxnId));
        if (targetTxnIds.isEmpty()) {
          // Idempotent case where txn was already closed or commit txn event received without
          // corresponding open txn event.
          LOG.info("Target txn id is missing for source txn id : " + sourceTxnId +
                       " and repl policy " + rqst.getReplPolicy());
          return;
        }
        assert targetTxnIds.size() == 1;
        txnId = targetTxnIds.get(0);
      }
      TxnTable txn = openOrAbortedTxnsById.get(txnId);
      if (txn == null) {
        //if here, txn was not found (in expected state)
        TxnHandler.TxnStatus actualTxnStatus = findTxnState(txn.id);
        if (actualTxnStatus == TxnHandler.TxnStatus.COMMITTED) {
          if (rqst.isSetReplPolicy()) {
            // in case of replication, idempotent is taken care by getTargetTxnId
            LOG.warn("Invalid state COMMITTED for transactions started using replication replay task");
          }
          /**
           * This makes the operation idempotent
           * (assume that this is most likely due to retry logic)
           */
          LOG.info("Nth commitTxn(" + JavaUtils.txnIdToString(txn.id) + ") msg");
          return;
        }
        TxnHandler.raiseTxnUnexpectedState(actualTxnStatus, txn.id);
      }
      Set<TxnComponentsTable> updatedOrDeletedComponents = new HashSet<>();
      long txnIdWhenCommitted = nextTxnId - 1;
      long commitTime = System.currentTimeMillis();
      // Collect the components where delete or update happened - also set the txnIdWhenCommitted
      txn.components.forEach(txnComponent -> {
        txnComponent.txnIdWhenCommitted = txnIdWhenCommitted;
        txnComponent.commitTime = commitTime;
        if (txnComponent.type == TxnHandler.OperationType.DELETE
                || txnComponent.type == TxnHandler.OperationType.UPDATE) {
          updatedOrDeletedComponents.add(txnComponent);
        }
      });
      // Check for write conflicts
      if (!rqst.isSetReplPolicy() && updatedOrDeletedComponents.size() > 0) {
        Optional<TxnComponentsTable> conflict = updatedOrDeletedComponents.stream().filter(txnComponent -> {
          TxnComponentsTable completedComponent = getLastCompletedWrite(txnComponent.database, txnComponent.table);
          return completedComponent != null && completedComponent.txnIdWhenCommitted >= txnComponent.txnId;
        }).findAny();
        // If we found a conflict
        if (!conflict.isPresent()) {
          TxnComponentsTable conflictingComponent = conflict.get();
          TxnComponentsTable committedComponent = getLastCompletedWrite(conflictingComponent.database, conflictingComponent.table);
          String msg = generateAbortMessage(conflictingComponent, committedComponent);
          if (abortTxns(Collections.singletonList(txn.id), true) != 1) {
            throw new IllegalStateException(msg + " FAILED!");
          }
          LOG.info(msg);
          throw new TxnAbortedException(msg);
        }
        // No conflict
        updatedOrDeletedComponents.forEach(txnComponent -> {
          updateLastCompletedWrite(txnComponent);
        });
      }
      if (!rqst.isSetReplPolicy()) {
        // Remove compacts, selects we do not need to remember them any more
        Iterator<TxnComponentsTable> components = txn.components.iterator();
        while (components.hasNext()) {
          TxnComponentsTable component = components.next();
          if (component.type == TxnHandler.OperationType.COMPACT
                  || component.type == TxnHandler.OperationType.SELECT) {
            components.remove();
          } else {
            updateLastCompletedWrite(component);
          }
        }
      } else {
        if (rqst.isSetWriteEventInfos()) {
          rqst.getWriteEventInfos().forEach(writeEvent -> {
            TxnComponentsTable component = new TxnComponentsTable(txn.id, txnIdWhenCommitted, commitTime, false, writeEvent);
            // We expect no other write sources for replicated tables, so we can update the writeId
            updateLastCompletedWrite(component);
          });
        }
        deleteReplTxnMapEntry(txn.id, rqst.getReplPolicy());
      }
      if (txn.components.size() > 0) {
        committedWriteTxnsById.put(txn.id, txn);
      }
      openOrAbortedTxnsById.remove(txn.id);
      openOrAbortedTxns.remove(txn);
      removeLocks(locksByTnxId.get(txn.id));
      locksByTnxId.remove(txn.id);
      updateMinOpenedTxn(txn.id);
      materializationRebuildLocks.remove(txn.id);
      // TODO Update table params
      // TODO TransactionListener
    } finally {
      txnLock.unlock();
    }
  }

  @Override
  public void replTableWriteIdState(ReplTblWriteIdStateRequest rqst) throws MetaException {
    String database = rqst.getDbName().toLowerCase();
    String table = rqst.getTableName().toLowerCase();
    ValidWriteIdList validWriteIdList = new ValidReaderWriteIdList(rqst.getValidWriteIdlist());

    // Get the abortedWriteIds which are already sorted in ascending order.
    List<Long> abortedWriteIds = TxnHandler.getAbortedWriteIds(validWriteIdList);
    int numAbortedWrites = abortedWriteIds.size();
    txnLock.lock();
    try {
      if (getLastCompletedWrite(database, table) != null) {
        LOG.info("Idempotent flow: WriteId state <" + validWriteIdList + "> is already applied for the table: "
                     + database + "." + table);
        return;
      }
      if (numAbortedWrites > 0) {
        // Allocate/Map one txn per aborted writeId and abort the txn to mark writeid as aborted.
        List<Long> txnIds = openTxns(numAbortedWrites, rqst.getUser(), rqst.getHostName(), null, null);
        assert(numAbortedWrites == txnIds.size());

        // Map each aborted write id with each allocated txn.
        for(int i = 0; i < txnIds.size(); i++) {
          TxnTable txn = openOrAbortedTxnsById.get(txnIds.get(i));
          TxnComponentsTable component = new TxnComponentsTable(txn.id, database, table, abortedWriteIds.get(i));
          txn.components = Collections.singleton(component);
          addRecentWrite(component);
        }

        // Abort all the allocated txns so that the mapped write ids are referred as aborted ones.
        int numAborts = abortTxns(txnIds, true);
        assert(numAborts == numAbortedWrites);
      }
    } finally {
      txnLock.unlock();
    }
  }

  @Override
  public Materialization getMaterializationInvalidationInfo(CreationMetadata creationMetadata, String validTxnListStr)
      throws MetaException {
    if (creationMetadata.getTablesUsed().isEmpty()) {
      // Bail out
      LOG.warn("Materialization creation metadata does not contain any table");
      return null;
    }

    // Parse validTxnList
    final ValidReadTxnList validTxnList =
        new ValidReadTxnList(validTxnListStr);

    // Parse validReaderWriteIdList from creation metadata
    final ValidTxnWriteIdList validReaderWriteIdList =
        new ValidTxnWriteIdList(creationMetadata.getValidTxnList());

    // TODO TxnHandler keeps longer tab if there was an update - until first major compaction
    boolean needRefresh = creationMetadata.getTablesUsed().stream().anyMatch(fullyQualifiedName -> {
      ValidWriteIdList tblValidWriteIdList = validReaderWriteIdList.getTableValidWriteIdList(fullyQualifiedName);
      TxnComponentsTable lastWrite = lastCompletedWritesMap.get(fullyQualifiedName);
      return lastWrite.updateOrDelete &&
                 (lastWrite.writeId > tblValidWriteIdList.getHighWatermark()
                      || !tblValidWriteIdList.isWriteIdValid(lastWrite.writeId));
    });
    return new Materialization(needRefresh);
  }

  @Override
  public long getTxnIdForWriteId(String database, String table, long writeId) throws MetaException {
    Optional<TxnComponentsTable> result = getRecentWriteList(database, table).stream()
                                              .filter(component -> component.writeId == writeId)
                                              .findAny();

    if (result.isPresent()) {
      return result.get().txnId;
    }

    return -1L;
  }

  @Override
  public LockResponse lockMaterializationRebuild(String database, String table, long txnId) throws MetaException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Acquiring lock for materialization rebuild with txnId={} for {}", txnId,
          Warehouse.getQualifiedName(database, table));
    }
    txnLock.lock();
    try {
      if (materializationRebuildLocks.values().stream()
              .anyMatch(lock -> lock.database == database && lock.table == table)) {
        LOG.info("Ignoring request to rebuild " + database + "/" + table +
                     " since it is already being rebuilt");
        return new LockResponse(txnId, LockState.NOT_ACQUIRED);
      }
      materializationRebuildLocks.put(txnId, new MaterializationRebuildLock(database, table));
      return new LockResponse(txnId, LockState.ACQUIRED);
    } finally {
      txnLock.unlock();
    }
  }

  @Override
  public boolean heartbeatLockMaterializationRebuild(String database, String table, long txnId) throws MetaException {
    txnLock.lock();
    try {
      MaterializationRebuildLock lock = materializationRebuildLocks.get(txnId);
      if (lock == null || !lock.database.equals(database) || !lock.table.equals(table)) {
        LOG.info("No lock found for rebuild of " + Warehouse.getQualifiedName(database, table) +
                     " when trying to heartbeat");
        // It could not be renewed, return that information
        return false;
      } else {
        lock.lastHearthbeatTime = System.currentTimeMillis();
        return true;
      }
    } finally {
      txnLock.unlock();
    }
  }

  @Override
  public long cleanupMaterializationRebuildLocks(ValidTxnList validTxnList, long timeout) throws MetaException {
    // TODO we do not really need the validTxnList - we want valid list really
    long timeoutTime = System.currentTimeMillis() - timeout;
    int originalSize = materializationRebuildLocks.size();
    txnLock.lock();
    try {
      materializationRebuildLocks.entrySet().removeIf( entry -> {
        if (entry.getValue().lastHearthbeatTime < timeoutTime) {
          TxnTable txn = openOrAbortedTxnsById.get(entry.getKey());
          LOG.debug("Hearthbeat timeout for {}", entry.getValue());
          return txn == null || txn.state != TxnState.OPEN;
        } else {
          return false;
        }
      });
      return materializationRebuildLocks.size() - originalSize;
    } finally {
      txnLock.unlock();
    }
  }

  @Override
  public GetValidWriteIdsResponse getValidWriteIds(GetValidWriteIdsRequest rqst) throws NoSuchTxnException, MetaException {
    txnLock.lock();
    try {
      ValidTxnList validTxnList;

      // We should prepare the valid write ids list based on validTxnList of current txn.
      // If no txn exists in the caller, then they would pass null for validTxnList and so it is
      // required to get the current state of txns to make validTxnList
      if (rqst.isSetValidTxnList()) {
        assert rqst.isSetWriteId() == false;
        validTxnList = new ValidReadTxnList(rqst.getValidTxnList());
      } else if (rqst.isSetWriteId()) {
        String[] names = TxnUtils.getDbTableName(rqst.getFullTableNames().get(0));
        validTxnList = TxnCommonUtils.createValidReadTxnList(getOpenTxns(), getTxnIdForWriteId(names[0], names[1], rqst.getWriteId()));
      } else {
        // Passing 0 for currentTxn means, this validTxnList is not wrt to any txn
        validTxnList = TxnCommonUtils.createValidReadTxnList(getOpenTxns(), 0);
      }

      // Get the valid write id list for all the tables read by the current txn
      List<TableValidWriteIds> tblValidWriteIdsList = new ArrayList<>();
      for (String fullTableName : rqst.getFullTableNames()) {
        String[] names = TxnUtils.getDbTableName(rqst.getFullTableNames().get(0));
        tblValidWriteIdsList.add(getValidWriteIdsForTable(names[0], names[1], validTxnList));
      }
      GetValidWriteIdsResponse owr = new GetValidWriteIdsResponse(tblValidWriteIdsList);

      return owr;
    } finally {
      txnLock.unlock();
    }
  }

  private static BitSet EMPTY_BIT_SET = new BitSet();
  // Method to get the Valid write ids list for the given table
  private TableValidWriteIds getValidWriteIdsForTable(String database, String table, ValidTxnList validTxnList) {
    txnLock.lock();
    try {
      // Need to initialize to 0 to make sure if nobody modified this table, then current txn
      // shouldn't read any data.
      // If there is a conversion from non-acid to acid table, then by default 0 would be assigned as
      // writeId for data from non-acid table and so writeIdHwm=0 would ensure those data are readable by any txns.
      long writeIdHwm = 0;
      List<Long> invalidWriteIdList = new LinkedList<>();
      long minOpenWriteId = Long.MAX_VALUE;
      BitSet abortedBits = new BitSet();
      long txnHwm = validTxnList.getHighWatermark();

      List<TxnComponentsTable> recentWriteList = getRecentWriteList(database, table);

      int i = 0;
      int writeIdHwmPos = 0;
      for(TxnComponentsTable write : recentWriteList) {
        if (validTxnList.isTxnValid(write.txnId)) {
          // Skip if the transaction under evaluation is already committed.
          continue;
        }
        if (write.txnId <= txnHwm && write.writeId > writeIdHwm) {
          // Find the writeId high water mark based upon txnId high water mark. If found, then, need to
          // traverse through all write Ids less than writeId HWM to make exceptions list.
          // The writeHWM = min(NEXT_WRITE_ID.nwi_next-1, max(TXN_TO_WRITE_ID.t2w_writeid under txnHwm))
          writeIdHwm = write.writeId;
          writeIdHwmPos = i;
        }
        // The current txn is either in open or aborted state.
        // Mark the write ids state as per the txn state.
        invalidWriteIdList.add(write.writeId);
        if (validTxnList.isTxnAborted(write.txnId)) {
          abortedBits.set(invalidWriteIdList.size() - 1);
        } else {
          minOpenWriteId = Math.min(minOpenWriteId, write.writeId);
        }
        i++;
      }

      if (writeIdHwmPos > 0) {
        // If we found relevant pending writes
        invalidWriteIdList = invalidWriteIdList.subList(0, writeIdHwmPos);
        abortedBits = abortedBits.get(0, writeIdHwmPos);
      } else {
        // No relevant pending writes
        minOpenWriteId = Long.MAX_VALUE;
        if (recentWriteList.size() == 0) {
          TxnComponentsTable lastWrite = getLastCompletedWrite(database, table);
          if (lastWrite != null) {
            writeIdHwm = lastWrite.writeId;
          } else {
            writeIdHwm = 0;
          }
          invalidWriteIdList = Collections.emptyList();
          abortedBits = EMPTY_BIT_SET;
        } else {
          TxnComponentsTable firstWrite = recentWriteList.get(0);
          invalidWriteIdList = Collections.singletonList(firstWrite.writeId);
          if (validTxnList.isTxnAborted(firstWrite.txnId)) {
            abortedBits.set(invalidWriteIdList.size() - 1);
          } else {
            minOpenWriteId = firstWrite.writeId;
          }
        }
      }

      ByteBuffer byteBuffer = ByteBuffer.wrap(abortedBits.toByteArray());
      TableValidWriteIds owi = new TableValidWriteIds(database + "." + table, writeIdHwm, invalidWriteIdList, byteBuffer);
      if (minOpenWriteId < Long.MAX_VALUE) {
        owi.setMinOpenWriteId(minOpenWriteId);
      }
      return owi;
    } finally {
      txnLock.unlock();
    }
  }


  @Override
  public AllocateTableWriteIdsResponse allocateTableWriteIds(AllocateTableWriteIdsRequest rqst) throws NoSuchTxnException, TxnAbortedException, MetaException {
    List<Long> txnIds;
    String database = rqst.getDbName().toLowerCase();
    String table = rqst.getTableName().toLowerCase();
    List<TxnToWriteId> txnToWriteIds = new ArrayList<>();
    List<TxnToWriteId> srcTxnToWriteIds = null;
    txnLock.lock();
    try {
      if (rqst.isSetReplPolicy()) {
        srcTxnToWriteIds = rqst.getSrcTxnToWriteIdList();
        List<Long> srcTxnIds = new ArrayList<>();
        assert (rqst.isSetSrcTxnToWriteIdList());
        assert (!rqst.isSetTxnIds());
        assert (!srcTxnToWriteIds.isEmpty());

        for (TxnToWriteId txnToWriteId :  srcTxnToWriteIds) {
          srcTxnIds.add(txnToWriteId.getTxnId());
        }
        txnIds = getTargetTxnIdList(rqst.getReplPolicy(), srcTxnIds);
        if (srcTxnIds.size() != txnIds.size()) {
          // Idempotent case where txn was already closed but gets allocate write id event.
          // So, just ignore it and return empty list.
          LOG.info("Idempotent case: Target txn id is missing for source txn id : " + srcTxnIds.toString() +
                       " and repl policy " + rqst.getReplPolicy());
          return new AllocateTableWriteIdsResponse(txnToWriteIds);
        }
      } else {
        assert (!rqst.isSetSrcTxnToWriteIdList());
        assert (rqst.isSetTxnIds());
        txnIds = rqst.getTxnIds();
      }

      //Easiest check since we can't differentiate do we handle singleton list or list with multiple txn ids.
      if(txnIds.size() > 1) {
        Collections.sort(txnIds); //easier to read logs and for assumption done in replication flow
      }

      // Check if all the input txns are in open state. Write ID should be allocated only for open transactions.
      if (txnIds.stream().anyMatch(txnId -> openOrAbortedTxnsById.get(txnId).state == TxnState.OPEN)) {
        throw new RuntimeException("This should never happen for txnIds: " + txnIds);
      }

      // Traverse the TXN_TO_WRITE_ID to see if any of the input txns already have allocated a
      // write id for the same db.table. If yes, then need to reuse it else have to allocate new one
      // The write id would have been already allocated in case of multi-statement txns where
      // first write on a table will allocate write id and rest of the writes should re-use it.
      long allocatedTxnsCount = 0;
      for(long txnId : txnIds) {
        TxnTable txn = openOrAbortedTxnsById.get(txnId);
        Optional<TxnComponentsTable> existingWrite = txn.components.stream()
            .filter(component -> component.type == TxnHandler.OperationType.INSERT
                                     || component.type == TxnHandler.OperationType.UPDATE
                                     || component.type == TxnHandler.OperationType.DELETE)
            .findAny();
        if (existingWrite.isPresent()) {
          // We have an existing write
          txnToWriteIds.add(new TxnToWriteId(txn.id, existingWrite.get().writeId));
        } else {
          long nextWriteId = 0;
          LinkedList<TxnComponentsTable> recentWrites = getRecentWriteList(database, table);
          if (recentWrites.size() > 0) {
            // Get the last open write
            nextWriteId = recentWrites.getLast().writeId + 1;
          }
          if (nextWriteId == 0) {
            // If there was compaction and no active writes
            nextWriteId = getLastCompletedWrite(database, table).writeId + 1;
          }

          if (rqst.isSetReplPolicy()) {
            // In replication flow, we always need to allocate write ID equal to that of source.
            assert(srcTxnToWriteIds != null);
            long srcWriteId = srcTxnToWriteIds.get(0).getWriteId();
            if (nextWriteId != srcWriteId && srcWriteId > 0) {
              // For repl flow, if the source write id is mismatching with target next write id, then current
              // metadata in TXN_TO_WRITE_ID is stale for this table and hence need to clean-up TXN_TO_WRITE_ID.
              // This is possible in case of first incremental repl after bootstrap where concurrent write
              // and drop table was performed at source during bootstrap dump.
              recentWrites.forEach(write -> {
                TxnTable oldTxn = openOrAbortedTxnsById.get(write.txnId);
                if (oldTxn == null) {
                  oldTxn = committedWriteTxnsById.get(write.txnId);
                }
                oldTxn.components.remove(write);
              });
              lastCompletedWritesMap.remove(database + "." + table);
            }
          }

          TxnComponentsTable newWrite = new TxnComponentsTable(txn.id, database, table, nextWriteId);
          addRecentWrite(newWrite);
          txn.components.add(newWrite);
          allocatedTxnsCount++;
          txnToWriteIds.add(new TxnToWriteId(txn.id, nextWriteId));
          LOG.info("Allocated writeID: " + nextWriteId + " for txnId: " + txn.id);
        }
      }
      // Batch allocation should always happen atomically. Either write ids for all txns is allocated or none.
      long numOfWriteIds = txnIds.size();
      assert ((allocatedTxnsCount == 0) || (numOfWriteIds == allocatedTxnsCount));
      if (allocatedTxnsCount == numOfWriteIds) {
        // If all the txns in the list have pre-allocated write ids for the given table, then just return.
        // This is for idempotent case.
        return new AllocateTableWriteIdsResponse(txnToWriteIds);
      }
      LOG.info("Allocated write ids for the table: " + database + "." + table);
      return new AllocateTableWriteIdsResponse(txnToWriteIds);
    } finally {
      txnLock.unlock();
    }
  }

  @Override
  public void seedWriteIdOnAcidConversion(InitializeTableWriteIdsRequest rqst) throws MetaException {
    txnLock.lock();
    try {
      //since this is on conversion from non-acid to acid, NEXT_WRITE_ID should not have an entry
      //for this table.  It also has a unique index in case 'should not' is violated
      assert getLastCompletedWrite(rqst.getDbName(), rqst.getTblName()) != null;
      addRecentWrite(new TxnComponentsTable(Long.MIN_VALUE, rqst.getDbName(), rqst.getTblName(), rqst.getSeedWriteId()));
    } finally {
      txnLock.unlock();
    }
  }

  @Override
  public LockResponse lock(LockRequest rqst) throws NoSuchTxnException, TxnAbortedException, MetaException {
    return null;
  }

  @Override
  public LockResponse checkLock(CheckLockRequest rqst) throws NoSuchTxnException, NoSuchLockException, TxnAbortedException, MetaException {
    return null;
  }

  @Override
  public void unlock(UnlockRequest rqst) throws NoSuchLockException, TxnOpenException, MetaException {

  }

  @Override
  public ShowLocksResponse showLocks(ShowLocksRequest rqst) throws MetaException {
    return null;
  }

  @Override
  public void heartbeat(HeartbeatRequest ids) throws NoSuchTxnException, NoSuchLockException, TxnAbortedException, MetaException {

  }

  @Override
  public HeartbeatTxnRangeResponse heartbeatTxnRange(HeartbeatTxnRangeRequest rqst) throws MetaException {
    return null;
  }

  @Override
  public CompactionResponse compact(CompactionRequest rqst) throws MetaException {
    return null;
  }

  @Override
  public ShowCompactResponse showCompact(ShowCompactRequest rqst) throws MetaException {
    return null;
  }

  @Override
  public void addDynamicPartitions(AddDynamicPartitions rqst) throws NoSuchTxnException, TxnAbortedException, MetaException {

  }

  @Override
  public void cleanupRecords(HiveObjectType type, Database db, Table table, Iterator<Partition> partitionIterator) throws MetaException {

  }

  @Override
  public void onRename(String oldCatName, String oldDbName, String oldTabName, String oldPartName, String newCatName, String newDbName, String newTabName, String newPartName) throws MetaException {

  }

  @Override
  public void performTimeOuts() {

  }

  @Override
  public Set<CompactionInfo> findPotentialCompactions(int maxAborted) throws MetaException {
    return null;
  }

  @Override
  public void updateCompactorState(CompactionInfo ci, long compactionTxnId) throws MetaException {

  }

  @Override
  public CompactionInfo findNextToCompact(String workerId) throws MetaException {
    return null;
  }

  @Override
  public void markCompacted(CompactionInfo info) throws MetaException {

  }

  @Override
  public List<CompactionInfo> findReadyToClean() throws MetaException {
    return null;
  }

  @Override
  public long findMinOpenTxnId() throws MetaException {
    return 0;
  }

  @Override
  public void markCleaned(CompactionInfo info) throws MetaException {

  }

  @Override
  public void markFailed(CompactionInfo info) throws MetaException {

  }

  @Override
  public void cleanTxnToWriteIdTable() throws MetaException {

  }

  @Override
  public void cleanEmptyAbortedTxns() throws MetaException {

  }

  @Override
  public void revokeFromLocalWorkers(String hostname) throws MetaException {

  }

  @Override
  public void revokeTimedoutWorkers(long timeout) throws MetaException {

  }

  @Override
  public List<String> findColumnsWithStats(CompactionInfo ci) throws MetaException {
    return null;
  }

  @Override
  public void purgeCompactionHistory() throws MetaException {

  }

  @Override
  public void performWriteSetGC() {

  }

  @Override
  public boolean checkFailedCompactions(CompactionInfo ci) throws MetaException {
    return false;
  }

  @Override
  public int numLocksInLockTable() throws SQLException, MetaException {
    return 0;
  }

  @Override
  public long setTimeout(long milliseconds) {
    return 0;
  }

  @Override
  public MutexAPI getMutexAPI() {
    return null;
  }

  @Override
  public void setHadoopJobId(String hadoopJobId, long id) {

  }

  @Override
  public void addWriteNotificationLog(AcidWriteEvent acidWriteEvent) throws MetaException {

  }

  @Override
  public void setConf(Configuration configuration) {
    dbTxnHandler.setConf(configuration);
    // TODO load the data
    maxOpenTxns = MetastoreConf.getIntVar(dbTxnHandler.getConf(), MetastoreConf.ConfVars.MAX_OPEN_TXNS);
    maxOpenTxnsBatchSize = MetastoreConf.getIntVar(dbTxnHandler.getConf(), MetastoreConf.ConfVars.TXN_MAX_OPEN_BATCH);
  }

  @Override
  public Configuration getConf() {
    return dbTxnHandler.getConf();
  }

  private static class TxnChangeComponent {
    private final Long txnId;
    private final String databaseName;
    private final String tableName;
    private final String partition;
    private final TxnHandler.OperationType operationType;
    private Long writeId;
    private Long txnIdWhenCommitted;

    public TxnChangeComponent(Long txnId, String databaseName, String tableName, String partition, TxnHandler.OperationType operationType) {
      this.txnId = txnId;
      this.databaseName = databaseName;
      this.tableName = tableName;
      this.partition = partition;
      this.operationType = operationType;
    }

    public Long getWriteId() {
      return writeId;
    }

    public void setWriteId(Long writeId) {
      this.writeId = writeId;
    }

    public Long getTxnIdWhenCommitted() {
      return txnIdWhenCommitted;
    }

    public void setTxnIdWhenCommitted(Long txnIdWhenCommitted) {
      this.txnIdWhenCommitted = txnIdWhenCommitted;
    }
  }

  // CREATE TABLE TXNS - TxnInfo + TXN_TYPE
  private class TxnTable {
    long id;
    TxnState state;
    String user;
    String hostname;
    String agentInfo;
    int heartbeatCount;
    String metaInfo;
    long startedTime;
    long lastHearthbeatTime;
    TxnType type;
    Set<TxnComponentsTable> components; // TXN_COMPONENTS

    TxnTable(long id, String user, String hostname, TxnType type, long currentTime) {
      this.id = id;
      this.user = user;
      this.hostname = hostname;
      this.heartbeatCount = 0;
      this.lastHearthbeatTime = currentTime;
      this.type = type;
      this.startedTime = currentTime;
      this.state = TxnState.OPEN;
    }

    TxnInfo toTxnInfo() {
      TxnInfo newInfo = new TxnInfo();
      newInfo.setId(id);
      newInfo.setState(state);
      newInfo.setUser(user);
      newInfo.setHostname(hostname);
      newInfo.setAgentInfo(agentInfo);
      newInfo.setHeartbeatCount(heartbeatCount);
      newInfo.setMetaInfo(metaInfo);
      newInfo.setStartedTime(startedTime);
      newInfo.setLastHeartbeatTime(lastHearthbeatTime);
      return newInfo;
    }
  }

  // CREATE TABLE TXN_COMPONENTS
  // CREATE TABLE COMPLETED_TXN_COMPONENTS
  // CREATE TABLE WRITE_SET
  private class TxnComponentsTable {
    long txnId;
    String database;
    String table;
    String partition; // Not set for WRITE_SET
    TxnHandler.OperationType type; // Not set for replicated writes
    boolean updateOrDelete; // Not set correctly for replicated writes
    long commitTime; // Just for COMPLETED_TXN_COMPONENTS
    long txnIdWhenCommitted; // Just for committed WRITE_SET
    long writeId;

    TxnComponentsTable(long txnId, long txnIdWhenCommitted, long commitTime, boolean updateOrDelete, WriteEventInfo writeEventInfo) {
      this.txnId = txnId;
      this.txnIdWhenCommitted = txnIdWhenCommitted;
      this.commitTime = commitTime;
      this.updateOrDelete = updateOrDelete;
      this.database = writeEventInfo.getDatabase();
      this.table = writeEventInfo.getTable();
      this.partition = writeEventInfo.getPartition();
      this.writeId = writeEventInfo.getWriteId();
    }

    TxnComponentsTable(long txnId, String database, String table, long writeId) {
      this.txnId = txnId;
      this.database = database;
      this.table = table;
      this.writeId = writeId;
      this.updateOrDelete = false;
    }
  }

  // CREATE TABLE MATERIALIZATION_REBUILD_LOCKS
  private class MaterializationRebuildLock {
    String database;
    String table;
    long lastHearthbeatTime;

    MaterializationRebuildLock(String database, String table) {
      this.database = database;
      this.table = table;
      this.lastHearthbeatTime = System.currentTimeMillis();
    }
  }

/*

  CREATE TABLE NEXT_TXN_ID (
      NTXN_NEXT NUMBER(19) NOT NULL
);

// ShowLocksResponseElement
  CREATE TABLE HIVE_LOCKS (
      HL_LOCK_EXT_ID NUMBER(19) NOT NULL,
  HL_LOCK_INT_ID NUMBER(19) NOT NULL,
  HL_TXNID NUMBER(19) NOT NULL,
  HL_DB VARCHAR2(128) NOT NULL,
  HL_TABLE VARCHAR2(128),
  HL_PARTITION VARCHAR2(767),
  HL_LOCK_STATE CHAR(1) NOT NULL,
  HL_LOCK_TYPE CHAR(1) NOT NULL,
  HL_LAST_HEARTBEAT NUMBER(19) NOT NULL,
  HL_ACQUIRED_AT NUMBER(19),
  HL_USER varchar(128) NOT NULL,
  HL_HOST varchar(128) NOT NULL,
  HL_HEARTBEAT_COUNT number(10),
  HL_AGENT_INFO varchar2(128),
  HL_BLOCKEDBY_EXT_ID number(19),
  HL_BLOCKEDBY_INT_ID number(19),
  PRIMARY KEY(HL_LOCK_EXT_ID, HL_LOCK_INT_ID)
) ROWDEPENDENCIES;

  CREATE TABLE NEXT_LOCK_ID (
      NL_NEXT NUMBER(19) NOT NULL
);

// CompactionInfo
  CREATE TABLE COMPACTION_QUEUE (
      CQ_ID NUMBER(19) PRIMARY KEY,
  CQ_DATABASE varchar(128) NOT NULL,
  CQ_TABLE varchar(128) NOT NULL,
  CQ_PARTITION varchar(767),
  CQ_STATE char(1) NOT NULL,
  CQ_TYPE char(1) NOT NULL,
  CQ_TBLPROPERTIES varchar(2048),
  CQ_WORKER_ID varchar(128),
  CQ_START NUMBER(19),
  CQ_RUN_AS varchar(128),
  CQ_HIGHEST_WRITE_ID NUMBER(19),
  CQ_META_INFO BLOB,
  CQ_HADOOP_JOB_ID varchar2(32)
) ROWDEPENDENCIES;

  CREATE TABLE NEXT_COMPACTION_QUEUE_ID (
      NCQ_NEXT NUMBER(19) NOT NULL
);

// Need new or CompactionInfo + CC_END
  CREATE TABLE COMPLETED_COMPACTIONS (
      CC_ID NUMBER(19) PRIMARY KEY,
  CC_DATABASE varchar(128) NOT NULL,
  CC_TABLE varchar(128) NOT NULL,
  CC_PARTITION varchar(767),
  CC_STATE char(1) NOT NULL,
  CC_TYPE char(1) NOT NULL,
  CC_TBLPROPERTIES varchar(2048),
  CC_WORKER_ID varchar(128),
  CC_START NUMBER(19),
  CC_END NUMBER(19),
  CC_RUN_AS varchar(128),
  CC_HIGHEST_WRITE_ID NUMBER(19),
  CC_META_INFO BLOB,
  CC_HADOOP_JOB_ID varchar2(32)
) ROWDEPENDENCIES;

// Map?
  CREATE TABLE TXN_TO_WRITE_ID (
      T2W_TXNID NUMBER(19) NOT NULL,
  T2W_DATABASE VARCHAR2(128) NOT NULL,
  T2W_TABLE VARCHAR2(256) NOT NULL,
  T2W_WRITEID NUMBER(19) NOT NULL
);

  CREATE TABLE NEXT_WRITE_ID (
      NWI_DATABASE VARCHAR2(128) NOT NULL,
  NWI_TABLE VARCHAR2(256) NOT NULL,
  NWI_NEXT NUMBER(19) NOT NULL
);

  CREATE TABLE MIN_HISTORY_LEVEL (
      MHL_TXNID NUMBER(19) NOT NULL,
  MHL_MIN_OPEN_TXNID NUMBER(19) NOT NULL,
  PRIMARY KEY(MHL_TXNID)
);

  CREATE TABLE MATERIALIZATION_REBUILD_LOCKS (
      MRL_TXN_ID NUMBER NOT NULL,
      MRL_DB_NAME VARCHAR(128) NOT NULL,
  MRL_TBL_NAME VARCHAR(256) NOT NULL,
  MRL_LAST_HEARTBEAT NUMBER NOT NULL,
  PRIMARY KEY(MRL_TXN_ID)
);

  CREATE TABLE REPL_TXN_MAP (
      RTM_REPL_POLICY varchar(256) NOT NULL,
  RTM_SRC_TXN_ID number(19) NOT NULL,
  RTM_TARGET_TXN_ID number(19) NOT NULL,
  PRIMARY KEY (RTM_REPL_POLICY, RTM_SRC_TXN_ID)
);


  CREATE TABLE TXN_WRITE_NOTIFICATION_LOG (
      WNL_ID number(19) NOT NULL,
  WNL_TXNID number(19) NOT NULL,
  WNL_WRITEID number(19) NOT NULL,
  WNL_DATABASE varchar(128) NOT NULL,
  WNL_TABLE varchar(128) NOT NULL,
  WNL_PARTITION varchar(767),
  WNL_TABLE_OBJ clob NOT NULL,
  WNL_PARTITION_OBJ clob,
  WNL_FILES clob,
  WNL_EVENT_TIME number(10) NOT NULL
);
*/
}
