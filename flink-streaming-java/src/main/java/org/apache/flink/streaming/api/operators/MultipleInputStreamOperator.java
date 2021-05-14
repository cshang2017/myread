package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;

import java.util.List;

/**
 * Interface for stream operators with multiple {@link Input}s.
 */
@PublicEvolving
public interface MultipleInputStreamOperator<OUT> extends StreamOperator<OUT> {
	List<Input> getInputs();
}
