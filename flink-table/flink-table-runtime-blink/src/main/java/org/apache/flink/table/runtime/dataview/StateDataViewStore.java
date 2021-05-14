package org.apache.flink.table.runtime.dataview;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.dataview.ListViewTypeInfo;
import org.apache.flink.table.dataview.MapViewTypeInfo;

/**
 * This interface contains methods for registering {@link StateDataView} with a managed store.
 */
public interface StateDataViewStore {

	/**
	 * Creates a state map view.
	 *
	 * @param stateName The name of underlying state of the map view
	 * @param mapViewTypeInfo The type of the map view
	 * @param <N> Type of the namespace
	 * @param <UK> Type of the keys in the map state
	 * @param <UV> Type of the values in the map state
	 * @return a keyed map state
	 */
	<N, UK, UV> StateMapView<N, UK, UV> getStateMapView(
		String stateName, MapViewTypeInfo<UK, UV> mapViewTypeInfo) throws Exception;

	/**
	 * Creates a state list view.
	 *
	 * @param stateName The name of underlying state of the list view
	 * @param listViewTypeInfo The type of the list view
	 * @param <N> Type of the namespace
	 * @param <V> Type of the elements in the list state
	 * @return a keyed list state
	 */
	<N, V> StateListView<N, V> getStateListView(
		String stateName, ListViewTypeInfo<V> listViewTypeInfo) throws Exception;

	/**
	 * Gets the context that contains information about the UDF's runtime, such as the
	 * parallelism of the function, the subtask index of the function, or the name of
	 * the of the task that executes the function.
	 */
	RuntimeContext getRuntimeContext();
}
