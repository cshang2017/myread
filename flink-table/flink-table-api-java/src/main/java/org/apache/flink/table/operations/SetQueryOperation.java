package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A set operation on two relations. It provides a way to union, intersect or subtract underlying
 * data sets/streams. Both relations must have equal schemas.
 */
@Internal
public class SetQueryOperation implements QueryOperation {

	private final QueryOperation leftOperation;
	private final QueryOperation rightOperation;

	private final SetQueryOperationType type;
	private final boolean all;

	public SetQueryOperation(
			QueryOperation leftOperation,
			QueryOperation rightOperation,
			SetQueryOperationType type,
			boolean all) {
		this.leftOperation = leftOperation;
		this.rightOperation = rightOperation;
		this.type = type;
		this.all = all;
	}

	/**
	 * Represent kind of this set operation.
	 * <ul>
	 *     <li><b>MINUS</b> returns records from the left relation that do not exist in the right relation</li>
	 *     <li><b>INTERSECT</b> returns records that exist in both relation</li>
	 *     <li><b>UNION</b> returns records from both relations as a single relation</li>
	 * </ul>
	 */
	public enum SetQueryOperationType {
		INTERSECT,
		MINUS,
		UNION
	}

	@Override
	public TableSchema getTableSchema() {
		return leftOperation.getTableSchema();
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> args = new LinkedHashMap<>();
		args.put("all", all);

		return OperationUtils.formatWithChildren(typeToString(), args, getChildren(), Operation::asSummaryString);
	}

	private String typeToString() {
		switch (type) {
			case INTERSECT:
				return "Intersect";
			case MINUS:
				return "Minus";
			case UNION:
				return "Union";
			default:
				throw new IllegalStateException("Unknown set operation type: " + type);
		}
	}

	@Override
	public <T> T accept(QueryOperationVisitor<T> visitor) {
		return visitor.visit(this);
	}

	@Override
	public List<QueryOperation> getChildren() {
		return Arrays.asList(leftOperation, rightOperation);
	}

	public SetQueryOperationType getType() {
		return type;
	}

	public boolean isAll() {
		return all;
	}
}
