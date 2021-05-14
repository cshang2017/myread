package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Base abstract implementation of {@link Input} interface intended to be used when extending
 * {@link AbstractStreamOperatorV2}.
 */
@Experimental
public abstract class AbstractInput<IN, OUT> implements Input<IN> {
	/**
	 * {@code KeySelector} for extracting a key from an element being processed. This is used to
	 * scope keyed state to a key. This is null if the operator is not a keyed operator.
	 *
	 * <p>This is for elements from the first input.
	 */
	@Nullable
	protected final KeySelector<?, ?> stateKeySelector;
	protected final AbstractStreamOperatorV2<OUT> owner;
	protected final int inputId;
	protected final Output<StreamRecord<OUT>> output;

	public AbstractInput(AbstractStreamOperatorV2<OUT> owner, int inputId) {
		checkArgument(inputId > 0, "Inputs are index from 1");
		this.owner = owner;
		this.inputId = inputId;
		this.stateKeySelector = owner.config.getStatePartitioner(inputId - 1, owner.getUserCodeClassloader());
		this.output = owner.output;
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		owner.reportWatermark(mark, inputId);
	}

	@Override
	public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
		owner.reportOrForwardLatencyMarker(latencyMarker);
	}

	@Override
	public void setKeyContextElement(StreamRecord record) throws Exception {
		owner.internalSetKeyContextElement(record, stateKeySelector);
	}
}
