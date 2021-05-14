

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.ResolvedExpression;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Table operation that joins two relational operations based on given condition.
 */
@Internal
public class JoinQueryOperation implements QueryOperation {

	private final QueryOperation left;
	private final QueryOperation right;
	private final JoinType joinType;
	private final ResolvedExpression condition;
	private final boolean correlated;
	private final TableSchema tableSchema;

	/**
	 * Specifies how the two Tables should be joined.
	 */
	public enum JoinType {
		INNER,
		LEFT_OUTER,
		RIGHT_OUTER,
		FULL_OUTER
	}

	public JoinQueryOperation(
			QueryOperation left,
			QueryOperation right,
			JoinType joinType,
			ResolvedExpression condition,
			boolean correlated) {
		this.left = left;
		this.right = right;
		this.joinType = joinType;
		this.condition = condition;
		this.correlated = correlated;

		this.tableSchema = calculateResultingSchema(left, right);
	}

	private TableSchema calculateResultingSchema(QueryOperation left, QueryOperation right) {
		TableSchema leftSchema = left.getTableSchema();
		TableSchema rightSchema = right.getTableSchema();
		int resultingSchemaSize = leftSchema.getFieldCount() + rightSchema.getFieldCount();
		String[] newFieldNames = new String[resultingSchemaSize];
		System.arraycopy(leftSchema.getFieldNames(), 0, newFieldNames, 0, leftSchema.getFieldCount());
		System.arraycopy(
			rightSchema.getFieldNames(),
			0,
			newFieldNames,
			leftSchema.getFieldCount(),
			rightSchema.getFieldCount());

		TypeInformation[] newFieldTypes = new TypeInformation[resultingSchemaSize];

		System.arraycopy(leftSchema.getFieldTypes(), 0, newFieldTypes, 0, leftSchema.getFieldCount());
		System.arraycopy(
			rightSchema.getFieldTypes(),
			0,
			newFieldTypes,
			leftSchema.getFieldCount(),
			rightSchema.getFieldCount());
		return new TableSchema(newFieldNames, newFieldTypes);
	}

	public JoinType getJoinType() {
		return joinType;
	}

	public ResolvedExpression getCondition() {
		return condition;
	}

	public boolean isCorrelated() {
		return correlated;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> args = new LinkedHashMap<>();
		args.put("joinType", joinType);
		args.put("condition", condition);
		args.put("correlated", correlated);

		return OperationUtils.formatWithChildren("Join", args, getChildren(), Operation::asSummaryString);
	}

	@Override
	public List<QueryOperation> getChildren() {
		return Arrays.asList(left, right);
	}

	@Override
	public <T> T accept(QueryOperationVisitor<T> visitor) {
		return visitor.visit(this);
	}
}
