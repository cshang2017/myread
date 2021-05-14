package org.apache.flink.table.runtime.context;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.data.RowData;

/**
 * A ExecutionContext contains information about the context in which functions are executed and
 * the APIs to create state.
 */
public interface ExecutionContext {

	RowData currentKey();

	void setCurrentKey(RowData key);

	RuntimeContext getRuntimeContext();
}
