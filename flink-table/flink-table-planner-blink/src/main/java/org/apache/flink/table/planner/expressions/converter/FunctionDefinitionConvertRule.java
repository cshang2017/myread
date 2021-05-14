package org.apache.flink.table.planner.expressions.converter;

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A call expression converter rule that converts calls to user defined functions.
 */
public class FunctionDefinitionConvertRule implements CallExpressionConvertRule {
	@Override
	public Optional<RexNode> convert(
			CallExpression call,
			ConvertContext context) {
		FunctionDefinition functionDefinition = call.getFunctionDefinition();

		if (functionDefinition instanceof BuiltInFunctionDefinition) {
			return Optional.empty();
		}

		TypeInference typeInference = functionDefinition.getTypeInference(context.getDataTypeFactory());
		if (typeInference.getOutputTypeStrategy() == TypeStrategies.MISSING) {
			return Optional.empty();
		}

		switch (functionDefinition.getKind()) {
			case SCALAR:
			case TABLE:
				List<RexNode> args = call.getChildren().stream().map(context::toRexNode).collect(Collectors.toList());

				final BridgingSqlFunction sqlFunction = BridgingSqlFunction.of(
					context.getDataTypeFactory(),
					context.getTypeFactory(),
					SqlKind.OTHER_FUNCTION,
					call.getFunctionIdentifier().orElse(null),
					functionDefinition,
					typeInference);

				return Optional.of(context.getRelBuilder().call(sqlFunction, args));
			default:
				return Optional.empty();
		}
	}
}
