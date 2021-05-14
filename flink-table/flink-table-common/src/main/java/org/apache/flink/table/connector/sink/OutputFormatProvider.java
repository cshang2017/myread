package org.apache.flink.table.connector.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.table.data.RowData;

/**
 * Provider of an {@link OutputFormat} instance as a runtime implementation for {@link DynamicTableSink}.
 */
@PublicEvolving
public interface OutputFormatProvider extends DynamicTableSink.SinkRuntimeProvider {

	/**
	 * Helper method for creating a static provider.
	 */
	static OutputFormatProvider of(OutputFormat<RowData> outputFormat) {
		return () -> outputFormat;
	}

	/**
	 * Creates an {@link OutputFormat} instance.
	 */
	OutputFormat<RowData> createOutputFormat();
}
