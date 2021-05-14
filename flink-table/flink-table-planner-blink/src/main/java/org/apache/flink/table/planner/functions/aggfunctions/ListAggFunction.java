
package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.types.DataType;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.concat;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.isNull;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.nullOf;

/**
 * built-in listagg aggregate function.
 */
public class ListAggFunction extends DeclarativeAggregateFunction {
	private int operandCount;
	private UnresolvedReferenceExpression acc = unresolvedRef("concatAcc");
	private UnresolvedReferenceExpression accDelimiter = unresolvedRef("accDelimiter");
	private Expression delimiter;
	private Expression operand;

	public ListAggFunction(int operandCount) {
		this.operandCount = operandCount;
		if (operandCount == 1) {
			delimiter = literal(",", DataTypes.STRING().notNull());
			operand = operand(0);
		} else {
			delimiter = operand(1);
			operand = operand(0);
		}
	}

	@Override
	public int operandCount() {
		return operandCount;
	}

	@Override
	public UnresolvedReferenceExpression[] aggBufferAttributes() {
		return new UnresolvedReferenceExpression[] { accDelimiter, acc };
	}

	@Override
	public DataType[] getAggBufferTypes() {
		return new DataType[] { DataTypes.STRING(), DataTypes.STRING() };
	}

	@Override
	public DataType getResultType() {
		return DataTypes.STRING();
	}

	@Override
	public Expression[] initialValuesExpressions() {
		return new Expression[] {
				/* delimiter */ literal(",", DataTypes.STRING().notNull()),
				/* acc */ nullOf(DataTypes.STRING())
		};
	}

	@Override
	public Expression[] accumulateExpressions() {
		return new Expression[] {
				/* delimiter */
				delimiter,
				/* acc */
				ifThenElse(isNull(operand), acc,
						ifThenElse(isNull(acc), operand, concat(concat(acc, delimiter), operand)))
		};
	}

	@Override
	public Expression[] retractExpressions() {
		throw new TableException("This function does not support retraction.");
	}

	@Override
	public Expression[] mergeExpressions() {
		return new Expression[] {
				/* delimiter */
				mergeOperand(accDelimiter),
				/* acc */
				ifThenElse(isNull(mergeOperand(acc)), acc,
						ifThenElse(isNull(acc), mergeOperand(acc),
								concat(concat(acc, mergeOperand(accDelimiter)), mergeOperand(acc))))
		};
	}

	@Override
	public Expression getValueExpression() {
		return acc;
	}
}
