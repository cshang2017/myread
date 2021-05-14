package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.ResolvedExpression;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Expresses sort operation of rows of the underlying relational operation with given order.
 * It also allows specifying offset and number of rows to fetch from the sorted data set/stream.
 */
@Internal
public class SortQueryOperation implements QueryOperation {

	private final List<ResolvedExpression> order;
	private final QueryOperation child;
	private final int offset;
	private final int fetch;

	public SortQueryOperation(
			List<ResolvedExpression> order,
			QueryOperation child) {
		this(order, child, -1, -1);
	}

	public SortQueryOperation(
			List<ResolvedExpression> order,
			QueryOperation child,
			int offset,
			int fetch) {
		this.order = order;
		this.child = child;
		this.offset = offset;
		this.fetch = fetch;
	}

	public List<ResolvedExpression> getOrder() {
		return order;
	}

	public QueryOperation getChild() {
		return child;
	}

	public int getOffset() {
		return offset;
	}

	public int getFetch() {
		return fetch;
	}

	@Override
	public TableSchema getTableSchema() {
		return child.getTableSchema();
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> args = new LinkedHashMap<>();
		args.put("order", order);
		args.put("offset", offset);
		args.put("fetch", fetch);

		return OperationUtils.formatWithChildren("Sort", args, getChildren(), Operation::asSummaryString);
	}

	@Override
	public List<QueryOperation> getChildren() {
		return Collections.singletonList(child);
	}

	@Override
	public <T> T accept(QueryOperationVisitor<T> visitor) {
		return visitor.visit(this);
	}
}
