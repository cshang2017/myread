package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.types.DataType;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.isNull;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.minus;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.plus;

/**
 * built-in count aggregate function.
 */
public class CountAggFunction extends DeclarativeAggregateFunction {
	private UnresolvedReferenceExpression count = unresolvedRef("count");

	@Override
	public int operandCount() {
		return 1;
	}

	@Override
	public UnresolvedReferenceExpression[] aggBufferAttributes() {
		return new UnresolvedReferenceExpression[] { count };
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
				/* count = */ literal(0L, getResultType().notNull())
		};
	}

	@Override
	public Expression[] accumulateExpressions() {
		return new Expression[] {
				/* count = */ ifThenElse(isNull(operand(0)), count, plus(count, literal(1L)))
		};
	}

	@Override
	public Expression[] retractExpressions() {
		return new Expression[] {
				/* count = */ ifThenElse(isNull(operand(0)), count, minus(count, literal(1L)))
		};
	}

	@Override
	public Expression[] mergeExpressions() {
		return new Expression[] {
				/* count = */ plus(count, mergeOperand(count))
		};
	}

	// If all input are nulls, count will be 0 and we will get result 0.
	@Override
	public Expression getValueExpression() {
		return count;
	}
}
