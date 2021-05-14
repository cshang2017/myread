package org.apache.flink.table.planner.sinks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.SelectTableSink;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.UUID;

/**
 * Basic implementation of {@link SelectTableSink}.
 */
public class SelectTableSinkBase implements SelectTableSink {

	private final TableSchema tableSchema;
	private final CollectSinkOperatorFactory<Row> factory;
	private final CollectResultIterator<Row> iterator;

	@SuppressWarnings("unchecked")
	public SelectTableSinkBase(TableSchema tableSchema) {
		this.tableSchema = SelectTableSinkSchemaConverter.convertTimeAttributeToRegularTimestamp(
			SelectTableSinkSchemaConverter.changeDefaultConversionClass(tableSchema));

		TypeSerializer<Row> typeSerializer = (TypeSerializer<Row>) TypeInfoDataTypeConverter
			.fromDataTypeToTypeInfo(this.tableSchema.toRowDataType())
			.createSerializer(new ExecutionConfig());
		String accumulatorName = "tableResultCollect_" + UUID.randomUUID();

		this.factory = new CollectSinkOperatorFactory<>(typeSerializer, accumulatorName);
		CollectSinkOperator<Row> operator = (CollectSinkOperator<Row>) factory.getOperator();
		this.iterator = new CollectResultIterator<>(operator.getOperatorIdFuture(), typeSerializer, accumulatorName);
	}

	@Override
	public DataType getConsumedDataType() {
		return tableSchema.toRowDataType();
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	protected DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
		CollectStreamSink<Row> sink = new CollectStreamSink<>(dataStream, factory);
		dataStream.getExecutionEnvironment().addOperator(sink.getTransformation());
		return sink.name("Select table sink");
	}

	@Override
	public void setJobClient(JobClient jobClient) {
		iterator.setJobClient(jobClient);
	}

	@Override
	public CloseableIterator<Row> getResultIterator() {
		return iterator;
	}
}
