package org.apache.flink.table.planner.expressions.converter;

import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import java.util.Optional;

/**
 * Rule to convert {@link CallExpression}.
 */
public interface CallExpressionConvertRule {

	/**
	 * Convert call expression with context to RexNode.
	 *
	 * @return Success return RexNode of {@link Optional#of}, Fail return {@link Optional#empty()}.
	 */
	Optional<RexNode> convert(CallExpression call, ConvertContext context);

	/**
	 * Context of {@link CallExpressionConvertRule}.
	 */
	interface ConvertContext {

		/**
		 * Convert expression to RexNode, used by children conversion.
		 */
		RexNode toRexNode(Expression expr);

		RelBuilder getRelBuilder();

		FlinkTypeFactory getTypeFactory();

		DataTypeFactory getDataTypeFactory();
	}
}
