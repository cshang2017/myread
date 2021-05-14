package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A reference to a field in an input. The reference contains:
 * <ul>
 *     <li>type</li>
 *     <li>index of an input the field belongs to</li>
 *     <li>index of a field within the corresponding input</li>
 * </ul>
 */
@PublicEvolving
public final class FieldReferenceExpression implements ResolvedExpression {

	private final String name;

	private final DataType dataType;

	/**
	 * index of an input the field belongs to.
	 * e.g. for a join, `inputIndex` of left input is 0 and `inputIndex` of right input is 1.
	 */
	private final int inputIndex;

	/**
	 * index of a field within the corresponding input.
	 */
	private final int fieldIndex;

	public FieldReferenceExpression(
			String name,
			DataType dataType,
			int inputIndex,
			int fieldIndex) {
		Preconditions.checkArgument(inputIndex >= 0, "Index of input should be a positive number");
		Preconditions.checkArgument(fieldIndex >= 0, "Index of field should be a positive number");
		this.name = Preconditions.checkNotNull(name, "Field name must not be null.");
		this.dataType = Preconditions.checkNotNull(dataType, "Field data type must not be null.");
		this.inputIndex = inputIndex;
		this.fieldIndex = fieldIndex;
	}

	public String getName() {
		return name;
	}

	public int getInputIndex() {
		return inputIndex;
	}

	public int getFieldIndex() {
		return fieldIndex;
	}

	@Override
	public DataType getOutputDataType() {
		return dataType;
	}

	@Override
	public List<ResolvedExpression> getResolvedChildren() {
		return Collections.emptyList();
	}

	@Override
	public String asSummaryString() {
		return name;
	}

	@Override
	public List<Expression> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <R> R accept(ExpressionVisitor<R> visitor) {
		return visitor.visit(this);
	}


	@Override
	public String toString() {
		return asSummaryString();
	}
}
