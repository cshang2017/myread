package org.apache.flink.table.runtime.arrow.sources;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * An Arrow TableSource which takes {@link Row} as the type of the produced records.
 */
@Internal
public class RowArrowTableSource extends AbstractArrowTableSource<Row> {
	public RowArrowTableSource(DataType dataType, byte[][] arrowData) {
		super(dataType, arrowData);
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		return execEnv.addSource(new RowArrowSourceFunction(dataType, arrowData));
	}
}
