package org.apache.flink.table.connector.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.table.data.RowData;

/**
 * Provider of an {@link InputFormat} instance as a runtime implementation for {@link ScanTableSource}.
 */
@PublicEvolving
public interface InputFormatProvider extends ScanTableSource.ScanRuntimeProvider {

	/**
	 * Helper method for creating a static provider.
	 */
	static InputFormatProvider of(InputFormat<RowData, ?> inputFormat) {
		return new InputFormatProvider() {
			@Override
			public InputFormat<RowData, ?> createInputFormat() {
				return inputFormat;
			}

			@Override
			public boolean isBounded() {
				return true;
			}
		};
	}

	/**
	 * Creates an {@link InputFormat} instance.
	 */
	InputFormat<RowData, ?> createInputFormat();
}
