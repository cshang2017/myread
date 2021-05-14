package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The report of currently allocated slots from a given TaskExecutor by a JobMaster.
 * This report is sent periodically to the TaskExecutor in order to reconcile the internal state of slot allocations.
 */
public class AllocatedSlotReport implements Serializable {

	private final JobID jobId;

	/** The allocated slots in slot pool. */
	private final Collection<AllocatedSlotInfo> allocatedSlotInfos;

	public AllocatedSlotReport(JobID jobId, Collection<AllocatedSlotInfo> allocatedSlotInfos) {
		this.jobId = checkNotNull(jobId);
		this.allocatedSlotInfos = checkNotNull(allocatedSlotInfos);
	}

	public JobID getJobId() {
		return jobId;
	}

	public Collection<AllocatedSlotInfo> getAllocatedSlotInfos() {
		return Collections.unmodifiableCollection(allocatedSlotInfos);
	}

}
