package org.apache.flink.runtime.broadcast;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;

import java.util.ArrayList;
import java.util.List;

/**
 * The default {@link BroadcastVariableInitializer} implementation that initializes the broadcast variable into a list.
 */
public class DefaultBroadcastVariableInitializer<T> implements BroadcastVariableInitializer<T, List<T>> {

	@Override
	public List<T> initializeBroadcastVariable(Iterable<T> data) {
		ArrayList<T> list = new ArrayList<T>();

		for (T value : data) {
			list.add(value);
		}
		return list;
	}

	// --------------------------------------------------------------------------------------------

	private static final DefaultBroadcastVariableInitializer<Object> INSTANCE = new DefaultBroadcastVariableInitializer<Object>();

	@SuppressWarnings("unchecked")
	public static <E> DefaultBroadcastVariableInitializer<E> instance() {
		return (DefaultBroadcastVariableInitializer<E>) INSTANCE;
	}

	private DefaultBroadcastVariableInitializer() {}
}
