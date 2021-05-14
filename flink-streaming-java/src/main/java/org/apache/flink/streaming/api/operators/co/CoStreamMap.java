

package org.apache.flink.streaming.api.operators.co;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * {@link org.apache.flink.streaming.api.operators.StreamOperator} for processing
 * {@link CoMapFunction CoMapFunctions}.
 */
@Internal
public class CoStreamMap<IN1, IN2, OUT>
		extends AbstractUdfStreamOperator<OUT, CoMapFunction<IN1, IN2, OUT>>
		implements TwoInputStreamOperator<IN1, IN2, OUT> {

	private static final long serialVersionUID = 1L;

	public CoStreamMap(CoMapFunction<IN1, IN2, OUT> mapper) {
		super(mapper);
	}

	@Override
	public void processElement1(StreamRecord<IN1> element) throws Exception {
		output.collect(element.replace(userFunction.map1(element.getValue())));
	}

	@Override
	public void processElement2(StreamRecord<IN2> element) throws Exception {
		output.collect(element.replace(userFunction.map2(element.getValue())));
	}
}
