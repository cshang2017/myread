package org.apache.flink.table.planner.functions;

import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.types.inference.TypeStrategies;

import static org.apache.flink.table.functions.FunctionKind.SCALAR;

/**
 * Dictionary of function definitions for all internal used functions.
 */
public class InternalFunctionDefinitions {

	public static final BuiltInFunctionDefinition THROW_EXCEPTION =
		new BuiltInFunctionDefinition.Builder()
			.name("throwException")
			.kind(SCALAR)
			.outputTypeStrategy(TypeStrategies.MISSING)
			.build();

}
