package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * A {@link StreamOperator} for executing {@link MapFunction MapFunctions}.
 */
@Internal
public class StreamMap<IN, OUT>
		extends AbstractUdfStreamOperator<OUT, MapFunction<IN, OUT>>
		implements OneInputStreamOperator<IN, OUT> {

	public StreamMap(MapFunction<IN, OUT> mapper) {
		super(mapper);
		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		output.collect(element.replace(userFunction.map(element.getValue())));
	}
}
