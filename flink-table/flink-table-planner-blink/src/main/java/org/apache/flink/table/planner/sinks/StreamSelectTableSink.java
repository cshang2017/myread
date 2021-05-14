package org.apache.flink.table.planner.sinks;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.SelectTableSink;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.types.Row;

/**
 * A {@link SelectTableSink} for streaming select job.
 *
 * <p><strong>NOTE:</strong>
 * Currently, only insert changes (AppendStreamTableSink) is supported.
 * Once FLINK-16998 is finished, all kinds of changes will be supported.
 */
public class StreamSelectTableSink extends SelectTableSinkBase implements AppendStreamTableSink<Row> {

	public StreamSelectTableSink(TableSchema tableSchema) {
		super(tableSchema);
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
		return super.consumeDataStream(dataStream);
	}
}
