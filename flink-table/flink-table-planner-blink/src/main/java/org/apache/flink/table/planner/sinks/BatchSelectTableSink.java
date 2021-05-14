
package org.apache.flink.table.planner.sinks;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.SelectTableSink;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.types.Row;


/**
 * A {@link SelectTableSink} for batch select job.
 */
public class BatchSelectTableSink extends SelectTableSinkBase implements StreamTableSink<Row> {

	public BatchSelectTableSink(TableSchema tableSchema) {
		super(tableSchema);
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
		return super.consumeDataStream(dataStream);
	}
}
