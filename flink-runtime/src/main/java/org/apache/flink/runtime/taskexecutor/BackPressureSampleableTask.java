package org.apache.flink.runtime.taskexecutor;

/**
 * Task interface used by {@link BackPressureSampleService} for back pressure tracking.
 */
public interface BackPressureSampleableTask {

	boolean isRunning();

	boolean isBackPressured();

}
