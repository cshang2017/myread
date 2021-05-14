package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.TableSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Inline scan of a {@link TableSource}. Used only when a {@link org.apache.flink.table.api.Table} was created
 * from {@link org.apache.flink.table.api.TableEnvironment#fromTableSource(TableSource)}.
 */
@Internal
public class TableSourceQueryOperation<T> implements QueryOperation {

	private final TableSource<T> tableSource;
	// Flag that tells if the tableSource is BatchTableSource.
	private final boolean isBatch;

	public TableSourceQueryOperation(TableSource<T> tableSource, boolean isBatch) {
		this.tableSource = tableSource;
		this.isBatch = isBatch;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSource.getTableSchema();
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> args = new HashMap<>();
		args.put("fields", tableSource.getTableSchema().getFieldNames());

		return OperationUtils.formatWithChildren("TableSource", args, getChildren(), Operation::asSummaryString);
	}

	public TableSource<T> getTableSource() {
		return tableSource;
	}

	public boolean isBatch() {
		return isBatch;
	}

	@Override
	public List<QueryOperation> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <R> R accept(QueryOperationVisitor<R> visitor) {
		return visitor.visit(this);
	}
}
