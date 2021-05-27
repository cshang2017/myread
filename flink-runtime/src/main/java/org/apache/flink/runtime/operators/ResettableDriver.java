package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.functions.Function;


/**
 * This interface marks a {@code Driver} as resettable, meaning that will reset part of their internal state but
 * otherwise reuse existing data structures.
 *
 * @see Driver
 * @see TaskContext
 * 
 * @param <S> The type of stub driven by this driver.
 * @param <OT> The data type of the records produced by this driver.
 */
public interface ResettableDriver<S extends Function, OT> extends Driver<S, OT> {
	
	boolean isInputResettable(int inputNum);
	
	void initialize() throws Exception;
	
	void reset() throws Exception;
	
	void teardown() throws Exception;
}
