package org.apache.flink.runtime.taskexecutor.partition;

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.util.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe Utility for tracking partitions.
 */
@ThreadSafe
public class PartitionTable<K> {

	private final Map<K, Set<ResultPartitionID>> trackedPartitionsPerKey = new ConcurrentHashMap<>(8);

	/**
	 * Returns whether any partitions are being tracked for the given key.
	 */
	public boolean hasTrackedPartitions(K key) {
		return trackedPartitionsPerKey.containsKey(key);
	}

	/**
	 * Starts the tracking of the given partition for the given key.
	 */
	public void startTrackingPartitions(K key, Collection<ResultPartitionID> newPartitionIds) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(newPartitionIds);

		if (newPartitionIds.isEmpty()) {
			return;
		}

		trackedPartitionsPerKey.compute(key, (ignored, partitionIds) -> {
			if (partitionIds == null) {
				partitionIds = new HashSet<>(8);
			}
			partitionIds.addAll(newPartitionIds);
			return partitionIds;
		});
	}

	/**
	 * Stops the tracking of all partition for the given key.
	 */
	public Collection<ResultPartitionID> stopTrackingPartitions(K key) {
		Preconditions.checkNotNull(key);

		Set<ResultPartitionID> storedPartitions = trackedPartitionsPerKey.remove(key);
		return storedPartitions == null
			? Collections.emptyList()
			: storedPartitions;
	}

	/**
	 * Stops the tracking of the given set of partitions for the given key.
	 */
	public void stopTrackingPartitions(K key, Collection<ResultPartitionID> partitionIds) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(partitionIds);

		// If the key is unknown we do not fail here, in line with ShuffleEnvironment#releaseFinishedPartitions
		trackedPartitionsPerKey.computeIfPresent(
			key,
			(ignored, resultPartitionIDS) -> {
				resultPartitionIDS.removeAll(partitionIds);
				return resultPartitionIDS.isEmpty()
					? null
					: resultPartitionIDS;
			});
	}
}
