

package org.apache.flink.runtime.accumulators;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * This class encapsulates a map of accumulators for a single task. It is used
 * for the transfer from TaskManagers to the JobManager and from the JobManager
 * to the Client.
 */
public class AccumulatorSnapshot implements Serializable {


	private final JobID jobID;
	private final ExecutionAttemptID executionAttemptID;

	/**
	 * Serialized user accumulators which may require the custom user class loader.
	 */
	private final SerializedValue<Map<String, Accumulator<?, ?>>> userAccumulators;

	public AccumulatorSnapshot(JobID jobID, ExecutionAttemptID executionAttemptID,
							Map<String, Accumulator<?, ?>> userAccumulators) throws IOException {
		this.jobID = jobID;
		this.executionAttemptID = executionAttemptID;
		this.userAccumulators = new SerializedValue<>(userAccumulators);
	}

	public JobID getJobID() {
		return jobID;
	}

	public ExecutionAttemptID getExecutionAttemptID() {
		return executionAttemptID;
	}

	/**
	 * Gets the user-defined accumulators values.
	 * @return the serialized map
	 */
	public Map<String, Accumulator<?, ?>> deserializeUserAccumulators(ClassLoader classLoader) throws IOException, ClassNotFoundException {
		return userAccumulators.deserializeValue(classLoader);
	}
}
