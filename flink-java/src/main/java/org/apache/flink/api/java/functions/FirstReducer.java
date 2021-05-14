

package org.apache.flink.api.java.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

/**
 * Reducer that only emits the first N elements in a group.
 * @param <T>
 */
@Internal
public class FirstReducer<T> implements GroupReduceFunction<T, T>, GroupCombineFunction<T, T> {

	private final int count;

	public FirstReducer(int n) {
		this.count = n;
	}

	@Override
	public void reduce(Iterable<T> values, Collector<T> out) throws Exception {

		int emitCnt = 0;
		for (T val : values) {
			out.collect(val);

			emitCnt++;
			if (emitCnt == count) {
				break;
			}
		}
	}

	@Override
	public void combine(Iterable<T> values, Collector<T> out) throws Exception {
		reduce(values, out);
	}

}
