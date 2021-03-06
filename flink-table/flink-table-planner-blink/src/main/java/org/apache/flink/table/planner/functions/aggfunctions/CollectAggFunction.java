package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.dataview.MapViewTypeInfo;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.util.WrappingRuntimeException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Aggregate function for COLLECT.
 * @param <T> type of collect element.
 */
public class CollectAggFunction<T>
	extends AggregateFunction<Map<T, Integer>, CollectAggFunction.CollectAccumulator<T>> {

	private final TypeInformation<T> elementType;

	public CollectAggFunction(TypeInformation<T> elementType) {
		this.elementType = elementType;
	}

	/** The initial accumulator for Collect aggregate function. */
	public static class CollectAccumulator<T> {
		public MapView<T, Integer> map = null;

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			CollectAccumulator<?> that = (CollectAccumulator<?>) o;
			return Objects.equals(map, that.map);
		}
	}

	public CollectAccumulator<T> createAccumulator() {
		CollectAccumulator<T> acc = new CollectAccumulator<>();
		acc.map = new MapView<>(elementType, Types.INT);
		return acc;
	}

	public void resetAccumulator(CollectAccumulator<T> accumulator) {
		accumulator.map.clear();
	}

	public void accumulate(CollectAccumulator<T> accumulator, T value) throws Exception {
		if (value != null) {
			Integer count = accumulator.map.get(value);
			if (count != null) {
				accumulator.map.put(value, count + 1);
			} else {
				accumulator.map.put(value, 1);
			}
		}
	}

	public void retract(CollectAccumulator<T> accumulator, T value) throws Exception {
		if (value != null) {
			Integer count = accumulator.map.get(value);
			if (count != null) {
				if (count == 1) {
					accumulator.map.remove(value);
				} else {
					accumulator.map.put(value, count - 1);
				}
			} else {
				accumulator.map.put(value, -1);
			}
		}
	}

	public void merge(CollectAccumulator<T> accumulator, Iterable<CollectAccumulator<T>> others) throws Exception {
		for (CollectAccumulator<T> other : others) {
			for (Map.Entry<T, Integer> entry : other.map.entries()) {
				T key = entry.getKey();
				Integer newCount = entry.getValue();
				Integer oldCount = accumulator.map.get(key);
				if (oldCount == null) {
					accumulator.map.put(key, newCount);
				} else {
					accumulator.map.put(key, oldCount + newCount);
				}
			}
		}
	}

	@Override
	public Map<T, Integer> getValue(CollectAccumulator<T> accumulator) {
		Map<T, Integer> result = new HashMap<>();
			for (Map.Entry<T, Integer> entry : accumulator.map.entries()) {
				result.put(entry.getKey(), entry.getValue());
			}
			return result;
	}

	@Override
	public TypeInformation<Map<T, Integer>> getResultType() {
		return new MapTypeInfo<>(elementType, Types.INT);
	}

	@Override
	@SuppressWarnings("unchecked")
	public TypeInformation<CollectAccumulator<T>> getAccumulatorType() {
			Class<CollectAccumulator<T>> clazz = (Class<CollectAccumulator<T>>) (Class) CollectAccumulator.class;
			List<PojoField> pojoFields = new ArrayList<>();
			pojoFields.add(new PojoField(
				clazz.getDeclaredField("map"),
				new MapViewTypeInfo<>(elementType, BasicTypeInfo.INT_TYPE_INFO)));
			return new PojoTypeInfo<>(clazz, pojoFields);
	}
}
