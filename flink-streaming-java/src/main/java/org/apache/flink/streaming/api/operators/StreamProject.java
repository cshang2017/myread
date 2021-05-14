package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * A {@link StreamOperator} for executing projections on streams.
 */
@Internal
public class StreamProject<IN, OUT extends Tuple>
		extends AbstractStreamOperator<OUT>
		implements OneInputStreamOperator<IN, OUT> {

	private TypeSerializer<OUT> outSerializer;
	private int[] fields;
	private int numFields;

	private transient OUT outTuple;

	public StreamProject(int[] fields, TypeSerializer<OUT> outSerializer) {
		this.fields = fields;
		this.numFields = this.fields.length;
		this.outSerializer = outSerializer;

		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		for (int i = 0; i < this.numFields; i++) {
			outTuple.setField(((Tuple) element.getValue()).getField(fields[i]), i);
		}
		output.collect(element.replace(outTuple));
	}

	@Override
	public void open() throws Exception {
		super.open();
		outTuple = outSerializer.createInstance();
	}
}
