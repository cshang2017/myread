package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * A report about the current values of all accumulators of the TaskExecutor for a given job.
 */
public class AccumulatorReport implements Serializable {
	private final Collection<AccumulatorSnapshot> accumulatorSnapshots;

	public AccumulatorReport(List<AccumulatorSnapshot> accumulatorSnapshots) {
		this.accumulatorSnapshots = accumulatorSnapshots;
	}

	public Collection<AccumulatorSnapshot> getAccumulatorSnapshots() {
		return accumulatorSnapshots;
	}
}
