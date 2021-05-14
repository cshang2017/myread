package org.apache.flink.cep.time;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.cep.functions.PatternProcessFunction;

/**
 * Enables access to time related characteristics such as current processing time or timestamp of currently processed
 * element. Used in {@link PatternProcessFunction} and
 * {@link org.apache.flink.cep.pattern.conditions.IterativeCondition}
 */
@PublicEvolving
public interface TimeContext {

	/**
	 * Timestamp of the element currently being processed.
	 *
	 * <p>In case of {@link org.apache.flink.streaming.api.TimeCharacteristic#ProcessingTime} this means the
	 * time when the event entered the cep operator.
	 */
	long timestamp();

	/** Returns the current processing time. */
	long currentProcessingTime();

}
