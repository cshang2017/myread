package org.apache.flink.runtime.client;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A variant of the {@link org.apache.flink.api.common.JobExecutionResult} that holds
 * its accumulator data in serialized form.
 */
public class SerializedJobExecutionResult implements java.io.Serializable {

	private final JobID jobId;

	private final Map<String, SerializedValue<OptionalFailure<Object>>> accumulatorResults;

	private final long netRuntime;

	/**
	 * Creates a new SerializedJobExecutionResult.
	 *
	 * @param jobID The job's ID.
	 * @param netRuntime The net runtime of the job (excluding pre-flight phase like the optimizer) in milliseconds
	 * @param accumulators A map of all accumulator results produced by the job, in serialized form
	 */
	public SerializedJobExecutionResult(
			JobID jobID,
			long netRuntime,
			Map<String, SerializedValue<OptionalFailure<Object>>> accumulators) {
		this.jobId = jobID;
		this.netRuntime = netRuntime;
		this.accumulatorResults = accumulators;
	}

	public JobID getJobId() {
		return jobId;
	}

	public long getNetRuntime() {
		return netRuntime;
	}

    /**
	 * Gets the net execution time of the job, i.e., the execution time in the parallel system,
	 * without the pre-flight steps like the optimizer in a desired time unit.
	 *
	 * @param desiredUnit the unit of the <tt>NetRuntime</tt>
	 * @return The net execution time in the desired unit.
	 */
	public long getNetRuntime(TimeUnit desiredUnit) {
		return desiredUnit.convert(getNetRuntime(), TimeUnit.MILLISECONDS);
	}

	public Map<String, SerializedValue<OptionalFailure<Object>>> getSerializedAccumulatorResults() {
		return this.accumulatorResults;
	}

	public JobExecutionResult toJobExecutionResult(ClassLoader loader) throws IOException, ClassNotFoundException {
		Map<String, OptionalFailure<Object>> accumulators =
				AccumulatorHelper.deserializeAccumulators(accumulatorResults, loader);

		return new JobExecutionResult(jobId, netRuntime, accumulators);
	}
}
