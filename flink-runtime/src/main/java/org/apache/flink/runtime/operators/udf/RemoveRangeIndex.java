package org.apache.flink.runtime.operators.udf;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class RemoveRangeIndex<T> implements MapFunction<Tuple2<Integer,T>,T> {

	@Override
	public T map(Tuple2<Integer, T> value) throws Exception {
		return value.f1;
	}
}
