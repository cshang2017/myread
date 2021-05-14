package org.apache.flink.table.planner.expressions.converter;

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;

import java.util.Optional;

import static org.apache.flink.table.planner.expressions.converter.ExpressionConverter.toRexNodes;

/**
 * {@link CallExpressionConvertRule} to convert {@link ScalarFunctionDefinition}.
 */
public class LegacyScalarFunctionConvertRule implements CallExpressionConvertRule {

	@Override
	public Optional<RexNode> convert(CallExpression call, ConvertContext context) {
		FunctionDefinition def = call.getFunctionDefinition();
		if (def instanceof ScalarFunctionDefinition) {
			ScalarFunction scalaFunc = ((ScalarFunctionDefinition) def).getScalarFunction();
			FunctionIdentifier identifier = call.getFunctionIdentifier()
				.orElse(FunctionIdentifier.of(scalaFunc.functionIdentifier()));
			SqlFunction sqlFunction = UserDefinedFunctionUtils.createScalarSqlFunction(
				identifier,
				scalaFunc.toString(),
				scalaFunc,
				context.getTypeFactory());
			return Optional.of(context.getRelBuilder()
				.call(sqlFunction, toRexNodes(context, call.getChildren())));
		}
		return Optional.empty();
	}
}
