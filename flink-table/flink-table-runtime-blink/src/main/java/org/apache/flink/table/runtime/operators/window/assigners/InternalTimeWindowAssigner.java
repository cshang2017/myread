package org.apache.flink.table.runtime.operators.window.assigners;

/**
 * The internal time window assigner which has some useful methods.
 */
public interface InternalTimeWindowAssigner {

	/**
	 * @return an InternalTimeWindowAssigner which in event time mode.
	 */
	InternalTimeWindowAssigner withEventTime();

	/**
	 * @return an InternalTimeWindowAssigner which in processing time mode.
	 */
	InternalTimeWindowAssigner withProcessingTime();
}
