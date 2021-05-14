package org.apache.flink.runtime.taskmanager;

/**
 * A dummy implementation of the {@link TaskActions} which is mainly used for tests.
 */
public class NoOpTaskActions implements TaskActions {

	@Override
	public void failExternally(Throwable cause) {}
}
