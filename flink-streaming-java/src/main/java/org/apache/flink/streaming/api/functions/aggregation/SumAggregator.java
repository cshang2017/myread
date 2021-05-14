package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.util.typeutils.FieldAccessor;
import org.apache.flink.streaming.util.typeutils.FieldAccessorFactory;

/**
 * An {@link AggregationFunction} that sums up fields.
 */
@Internal
public class SumAggregator<T> extends AggregationFunction<T> {

	private static final long serialVersionUID = 1L;

	private final FieldAccessor<T, Object> fieldAccessor;
	private final SumFunction adder;
	private final TypeSerializer<T> serializer;
	private final boolean isTuple;

	public SumAggregator(int pos, TypeInformation<T> typeInfo, ExecutionConfig config) {
		fieldAccessor = FieldAccessorFactory.getAccessor(typeInfo, pos, config);
		adder = SumFunction.getForClass(fieldAccessor.getFieldType().getTypeClass());
		if (typeInfo instanceof TupleTypeInfo) {
			isTuple = true;
			serializer = null;
		} else {
			isTuple = false;
			this.serializer = typeInfo.createSerializer(config);
		}
	}

	public SumAggregator(String field, TypeInformation<T> typeInfo, ExecutionConfig config) {
		fieldAccessor = FieldAccessorFactory.getAccessor(typeInfo, field, config);
		adder = SumFunction.getForClass(fieldAccessor.getFieldType().getTypeClass());
		if (typeInfo instanceof TupleTypeInfo) {
			isTuple = true;
			serializer = null;
		} else {
			isTuple = false;
			this.serializer = typeInfo.createSerializer(config);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public T reduce(T value1, T value2) throws Exception {
		if (isTuple) {
			Tuple result = ((Tuple) value1).copy();
			return fieldAccessor.set((T) result, adder.add(fieldAccessor.get(value1), fieldAccessor.get(value2)));
		} else {
			T result = serializer.copy(value1);
			return fieldAccessor.set(result, adder.add(fieldAccessor.get(value1), fieldAccessor.get(value2)));
		}
	}
}
