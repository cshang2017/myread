package org.apache.flink.runtime.accumulators;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Main accumulator registry which encapsulates user-defined accumulators.
 */
public class AccumulatorRegistry {

	protected final JobID jobID;
	protected final ExecutionAttemptID taskID;

	/* User-defined Accumulator values stored for the executing task. */
	private final Map<String, Accumulator<?, ?>> userAccumulators =
			new ConcurrentHashMap<>(4);

	public AccumulatorRegistry(JobID jobID, ExecutionAttemptID taskID) {
		this.jobID = jobID;
		this.taskID = taskID;
	}

	/**
	 * Creates a snapshot of this accumulator registry.
	 * @return a serialized accumulator map
	 */
	public AccumulatorSnapshot getSnapshot() {
			return new AccumulatorSnapshot(jobID, taskID, userAccumulators);
	}

	/**
	 * Gets the map for user-defined accumulators.
	 */
	public Map<String, Accumulator<?, ?>> getUserMap() {
		return userAccumulators;
	}

}
