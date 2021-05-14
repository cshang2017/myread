package org.apache.flink.table.runtime.arrow.sources;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/**
 * An Arrow TableSource which takes {@link RowData} as the type of the produced records.
 */
@Internal
public class ArrowTableSource extends AbstractArrowTableSource<RowData> {
	public ArrowTableSource(DataType dataType, byte[][] arrowData) {
		super(dataType, arrowData);
	}

	@Override
	public DataStream<RowData> getDataStream(StreamExecutionEnvironment execEnv) {
		return execEnv.addSource(new ArrowSourceFunction(dataType, arrowData));
	}
}
