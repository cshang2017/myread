package org.apache.flink.streaming.api.collector.selector;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Iterator;
import java.util.List;
import java.util.Set;


/**
 * Special version of {@link DirectedOutput} that performs a shallow copy of the
 * {@link StreamRecord} to ensure that multi-chaining works correctly.
 */
public class CopyingDirectedOutput<OUT> extends DirectedOutput<OUT> {

	@SuppressWarnings({"unchecked", "rawtypes"})
	public CopyingDirectedOutput(
			List<OutputSelector<OUT>> outputSelectors,
			List<? extends Tuple2<? extends Output<StreamRecord<OUT>>, StreamEdge>> outputs) {
		super(outputSelectors, outputs);
	}

	@Override
	public void collect(StreamRecord<OUT> record) {
		Set<Output<StreamRecord<OUT>>> selectedOutputs = selectOutputs(record);

		if (selectedOutputs.isEmpty()) {
			return;
		}

		Iterator<Output<StreamRecord<OUT>>> it = selectedOutputs.iterator();

		while (true) {
			Output<StreamRecord<OUT>> out = it.next();
			if (it.hasNext()) {
				// we don't have the last output
				// perform a shallow copy
				StreamRecord<OUT> shallowCopy = record.copy(record.getValue());
				out.collect(shallowCopy);
			} else {
				// this is the last output
				out.collect(record);
				break;
			}
		}
	}
}
