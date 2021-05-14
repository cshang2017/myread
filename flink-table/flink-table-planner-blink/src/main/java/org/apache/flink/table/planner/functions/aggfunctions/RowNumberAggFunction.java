package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.types.DataType;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.plus;

/**
 * built-in row_number aggregate function.
 */
public class RowNumberAggFunction extends DeclarativeAggregateFunction {
	private UnresolvedReferenceExpression sequence = unresolvedRef("seq");

	@Override
	public int operandCount() {
		return 0;
	}

	@Override
	public UnresolvedReferenceExpression[] aggBufferAttributes() {
		return new UnresolvedReferenceExpression[] { sequence };
	}

	@Override
	public DataType[] getAggBufferTypes() {
		return new DataType[] { DataTypes.BIGINT() };
	}

	@Override
	public DataType getResultType() {
		return DataTypes.BIGINT();
	}

	@Override
	public Expression[] initialValuesExpressions() {
		return new Expression[] {
				literal(0L)
		};
	}

	@Override
	public Expression[] accumulateExpressions() {
		return new Expression[] {
				plus(sequence, literal(1L))
		};
	}

	@Override
	public Expression[] retractExpressions() {
		throw new TableException("This function does not support retraction.");
	}

	@Override
	public Expression[] mergeExpressions() {
		throw new TableException("This function does not support merge.");
	}

	@Override
	public Expression getValueExpression() {
		return sequence;
	}
}
