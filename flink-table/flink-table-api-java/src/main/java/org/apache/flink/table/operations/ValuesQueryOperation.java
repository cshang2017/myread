

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Table operation that computes new table using given {@link Expression}s
 * from its input relational operation.
 */
@Internal
public class ValuesQueryOperation implements QueryOperation {

	private final List<List<ResolvedExpression>> values;
	private final TableSchema tableSchema;

	public ValuesQueryOperation(
			List<List<ResolvedExpression>> values,
			TableSchema tableSchema) {
		this.values = values;
		this.tableSchema = tableSchema;
	}

	public List<List<ResolvedExpression>> getValues() {
		return values;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> args = new LinkedHashMap<>();
		args.put("values", values);

		return OperationUtils.formatWithChildren("Values", args, getChildren(), Operation::asSummaryString);
	}

	@Override
	public List<QueryOperation> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <T> T accept(QueryOperationVisitor<T> visitor) {
		return visitor.visit(this);
	}


	@Override
	public String toString() {
		return asSummaryString();
	}
}
