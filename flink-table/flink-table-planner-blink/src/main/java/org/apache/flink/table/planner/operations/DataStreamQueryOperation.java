package org.apache.flink.table.planner.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.QueryOperationVisitor;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Describes a relational operation that reads from a {@link DataStream}.
 *
 * <p>This is only used for testing.
 *
 * <p>This operation may expose only part, or change the order of the fields available in a
 * {@link org.apache.flink.api.common.typeutils.CompositeType} of the underlying {@link DataStream}.
 * The {@link DataStreamQueryOperation#getFieldIndices()} describes the mapping between fields of the
 * {@link TableSchema} to the {@link org.apache.flink.api.common.typeutils.CompositeType}.
 */
@Internal
public class DataStreamQueryOperation<E> implements QueryOperation {

	private final ObjectIdentifier identifier;
	private final DataStream<E> dataStream;
	private final int[] fieldIndices;
	private final TableSchema tableSchema;
	// TODO remove this while TableSchema supports fieldNullables
	private final boolean[] fieldNullables;
	private final FlinkStatistic statistic;

	public DataStreamQueryOperation(
			ObjectIdentifier identifier,
			DataStream<E> dataStream,
			int[] fieldIndices,
			TableSchema tableSchema,
			boolean[] fieldNullables,
			FlinkStatistic statistic) {
		this.identifier = identifier;
		this.dataStream = dataStream;
		this.tableSchema = tableSchema;
		this.fieldNullables = fieldNullables;
		this.fieldIndices = fieldIndices;
		this.statistic = statistic;
	}

	public DataStream<E> getDataStream() {
		return dataStream;
	}

	public int[] getFieldIndices() {
		return fieldIndices;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> args = new LinkedHashMap<>();
		if (identifier != null) {
			args.put("id", identifier.asSummaryString());
		} else {
			args.put("id", dataStream.getId());
		}
		args.put("fields", tableSchema.getFieldNames());

		return OperationUtils.formatWithChildren("DataStream", args, getChildren(), Operation::asSummaryString);
	}

	@Override
	public List<QueryOperation> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <T> T accept(QueryOperationVisitor<T> visitor) {
		return visitor.visit(this);
	}

	public ObjectIdentifier getIdentifier() {
		return identifier;
	}

	public boolean[] getFieldNullables() {
		return fieldNullables;
	}

	public FlinkStatistic getStatistic() {
		return statistic;
	}
}
