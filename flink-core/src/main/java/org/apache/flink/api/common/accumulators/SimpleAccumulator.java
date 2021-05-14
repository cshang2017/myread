package org.apache.flink.api.common.accumulators;

import org.apache.flink.annotation.Public;

import java.io.Serializable;

/**
 * Similar to Accumulator, but the type of items to add and the result value
 * must be the same.
 */
@Public
public interface SimpleAccumulator<T extends Serializable> extends Accumulator<T,T> {}
