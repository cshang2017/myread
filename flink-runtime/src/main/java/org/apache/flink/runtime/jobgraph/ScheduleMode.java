package org.apache.flink.runtime.jobgraph;

/**
 * The ScheduleMode decides how tasks of an execution graph are started.
 */
public enum ScheduleMode {
	/** Schedule tasks lazily from the sources. Downstream tasks are started once their input data are ready */
	LAZY_FROM_SOURCES(true),

	/**
	 * Same as LAZY_FROM_SOURCES just with the difference that it uses batch slot requests which support the
	 * execution of jobs with fewer slots than requested. However, the user needs to make sure that the job
	 * does not contain any pipelined shuffles (every pipelined region can be executed with a single slot).
	 */
	LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST(true),

	/** Schedules all tasks immediately. */
	EAGER(false);

	private final boolean allowLazyDeployment;

	ScheduleMode(boolean allowLazyDeployment) {
		this.allowLazyDeployment = allowLazyDeployment;
	}

	/**
	 * Returns whether we are allowed to deploy consumers lazily.
	 */
	public boolean allowLazyDeployment() {
		return allowLazyDeployment;
	}
}
