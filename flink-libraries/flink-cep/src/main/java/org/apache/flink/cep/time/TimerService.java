package org.apache.flink.cep.time;

import org.apache.flink.annotation.Internal;

/**
 * Enables to provide time characteristic to {@link org.apache.flink.cep.nfa.NFA} for use in
 * {@link org.apache.flink.cep.pattern.conditions.IterativeCondition}.
 */
@Internal
public interface TimerService {

	/**
	 * Current processing time as returned from {@link org.apache.flink.streaming.api.TimerService}.
	 */
	long currentProcessingTime();

}
