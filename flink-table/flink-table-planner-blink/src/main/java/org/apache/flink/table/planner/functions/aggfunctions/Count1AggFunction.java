package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.types.DataType;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.minus;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.plus;

/**
 * This count1 aggregate function returns the count1 of values
 * which go into it like [[CountAggFunction]].
 * It differs in that null values are also counted.
 */
public class Count1AggFunction extends DeclarativeAggregateFunction {
	private UnresolvedReferenceExpression count1 = unresolvedRef("count1");

	@Override
	public int operandCount() {
		return 1;
	}

	@Override
	public UnresolvedReferenceExpression[] aggBufferAttributes() {
		return new UnresolvedReferenceExpression[] { count1 };
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
				/* count1 = */ literal(0L, getResultType().notNull())
		};
	}

	@Override
	public Expression[] accumulateExpressions() {
		return new Expression[] {
				/* count1 = */ plus(count1, literal(1L))
		};
	}

	@Override
	public Expression[] retractExpressions() {
		return new Expression[] {
				/* count1 = */ minus(count1, literal(1L))
		};
	}

	@Override
	public Expression[] mergeExpressions() {
		return new Expression[] {
				/* count1 = */ plus(count1, mergeOperand(count1))
		};
	}

	@Override
	public Expression getValueExpression() {
		return count1;
	}
}
