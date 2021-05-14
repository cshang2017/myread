package org.apache.flink.table.runtime.dataview;

import org.apache.flink.table.api.dataview.DataView;

/**
 * A {@link DataView} which is implemented using state backend.
 */
public interface StateDataView<N> extends DataView {

	/**
	 * Sets current namespace for state.
	 */
	void setCurrentNamespace(N namespace);

}
