package org.apache.flink.runtime.jobmanager.scheduler;

public enum Locality {

	/** No constraint existed on the task placement. */
	UNCONSTRAINED,

	/** The task was scheduled into the same TaskManager as requested */
	LOCAL,

	/** The task was scheduled onto the same host as requested */
	HOST_LOCAL,

	/** The task was scheduled to a destination not included in its locality preferences. */
	NON_LOCAL,

	/** No locality information was provided, it is unknown if the locality was respected */
	UNKNOWN
}
