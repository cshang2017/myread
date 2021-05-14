package org.apache.flink.runtime.taskexecutor;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A report about the current status of all slots of the TaskExecutor, describing
 * which slots are available and allocated, and what jobs (JobManagers) the allocated slots
 * have been allocated to.
 */
public class SlotReport implements Serializable, Iterable<SlotStatus> {

	/** The slots status of the TaskManager. */
	private final Collection<SlotStatus> slotsStatus;

	public SlotReport() {
		this(Collections.<SlotStatus>emptyList());
	}

	public SlotReport(SlotStatus slotStatus) {
		this(Collections.singletonList(slotStatus));
	}

	public SlotReport(final Collection<SlotStatus> slotsStatus) {
		this.slotsStatus = checkNotNull(slotsStatus);
	}

	public int getNumSlotStatus() {
		return slotsStatus.size();
	}

	@Override
	public Iterator<SlotStatus> iterator() {
		return slotsStatus.iterator();
	}

	@Override
	public String toString() {
		return "SlotReport{" +
			"slotsStatus=" + slotsStatus +
			'}';
	}
}
