/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.tez;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.split.SplitLocationProvider;
import org.apache.hive.common.util.Murmur3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;

/**
 * This maps a split (path + offset) to an index based on the number of locations provided.
 *
 * If locations do not change across jobs, the intention is to map the same split to the same node.
 *
 * A big problem is when nodes change (added, removed, temporarily removed and re-added) etc. That changes
 * the number of locations / position of locations - and will cause the cache to be almost completely invalidated.
 *
 * TODO: Support for consistent hashing when combining the split location generator and the ServiceRegistry.
 *
 */
public class HostAffinitySplitLocationProvider implements SplitLocationProvider {

  private final static Logger LOG = LoggerFactory.getLogger(
      HostAffinitySplitLocationProvider.class);
  private final boolean isDebugEnabled = LOG.isDebugEnabled();

  private final List<String> locations;
  private final Set<String> locationSet;
  private final int numberOfLocations;

  public HostAffinitySplitLocationProvider(List<String> knownLocations, int numberOfLocations) {
    Preconditions.checkState(knownLocations != null && !knownLocations.isEmpty(),
        HostAffinitySplitLocationProvider.class.getName() +
            " needs at least 1 location to function");
    Preconditions.checkArgument(numberOfLocations >= 0,
        HostAffinitySplitLocationProvider.class.getName() +
            " needs numberOfLocations at least set to 0. It is set to [" +
            numberOfLocations + "] now.");
    this.locations = knownLocations;
    this.locationSet = new HashSet<String>(knownLocations);
    this.numberOfLocations = numberOfLocations;
  }

  @Override
  public String[] getLocations(InputSplit split) throws IOException {
    if (!(split instanceof FileSplit)) {
      if (isDebugEnabled) {
        LOG.debug("Split: " + split + " is not a FileSplit. Using default locations");
      }
      return split.getLocations();
    }
    FileSplit fsplit = (FileSplit) split;
    String splitDesc = "Split at " + fsplit.getPath() + " with offset= " + fsplit.getStart()
        + ", length=" + fsplit.getLength();
    List<String> preferredLocations = new ArrayList<>(preferLocations(fsplit));
    List<String> finalLocations = new ArrayList<>(numberOfLocations);
    // Generate new preferred locations until we need more, or we do not have any preferred
    // location left
    while (finalLocations.size() < numberOfLocations && preferredLocations.size() > 0) {
      String nextLocation = preferredLocations.get(determineLocation(preferredLocations,
          fsplit.getPath().toString(), fsplit.getStart(), splitDesc));
      finalLocations.add(nextLocation);
      preferredLocations.remove(nextLocation);
    }
    return finalLocations.toArray(new String[0]);
  }

  private List<String> preferLocations(FileSplit fsplit) throws IOException {
    if (fsplit.getLocations() == null || fsplit.getLocations().length <= 0) {
      // Cloud FS
      return this.locations;
    }
    String[] datanodes = fsplit.getLocations();
    Arrays.sort(datanodes);
    ArrayList<String> targets = new ArrayList<String>(datanodes.length);
    for (String location : datanodes) {
      if (locationSet.contains(location)) {
        targets.add(location);
      }
    }
    if (targets.size() > 0) {
      return targets;
    }
    return this.locations;
  }

  @VisibleForTesting
  public static int determineLocation(
      List<String> locations, String path, long start, String desc) {
    if (locations.size() == 1) {
      // skip everything, this is simple
      return 0;
    }
    byte[] bytes = getHashInputForSplit(path, start);
    long hash1 = hash1(bytes);
    int index = Hashing.consistentHash(hash1, locations.size());
    String location = locations.get(index);
    if (LOG.isDebugEnabled()) {
      LOG.debug(desc + " mapped to index=" + index + ", location=" + location);
    }
    int iter = 1;
    long hash2 = 0;
    // Since our probing method is totally bogus, give up after some time.
    while (location == null && iter < locations.size() * 2) {
      if (iter == 1) {
        hash2 = hash2(bytes);
      }
      // Note that this is not real double hashing since we have consistent hash on top.
      index = Hashing.consistentHash(hash1 + iter * hash2, locations.size());
      location = locations.get(index);
      if (LOG.isDebugEnabled()) {
        LOG.debug(desc + " remapped to index=" + index + ", location=" + location);
      }
      ++iter;
    }
    return index;
  }

  private static byte[] getHashInputForSplit(String path, long start) {
    // Explicitly using only the start offset of a split, and not the length. Splits generated on
    // block boundaries and stripe boundaries can vary slightly. Try hashing both to the same node.
    // There is the drawback of potentially hashing the same data on multiple nodes though, when a
    // large split is sent to 1 node, and a second invocation uses smaller chunks of the previous
    // large split and send them to different nodes.
    byte[] pathBytes = path.getBytes();
    byte[] allBytes = new byte[pathBytes.length + 8];
    System.arraycopy(pathBytes, 0, allBytes, 0, pathBytes.length);
    SerDeUtils.writeLong(allBytes, pathBytes.length, start >> 3);
    return allBytes;
  }

  private static long hash1(byte[] bytes) {
    final int PRIME = 104729; // Same as hash64's default seed.
    return Murmur3.hash64(bytes, 0, bytes.length, PRIME);
  }

  private static long hash2(byte[] bytes) {
    final int PRIME = 1366661;
    return Murmur3.hash64(bytes, 0, bytes.length, PRIME);
  }
}
