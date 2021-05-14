package org.apache.flink.table.functions;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.functions.python.utils.PythonFunctionUtils;

/**
 * A util to instantiate {@link FunctionDefinition} in the default way.
 */
public class FunctionDefinitionUtil {

	public static FunctionDefinition createFunctionDefinition(String name, String className) {
		return createJavaFunctionDefinition(name, className);
	}

	public static FunctionDefinition createFunctionDefinition(
			String name,
			String className,
			FunctionLanguage functionLanguage,
			ReadableConfig config) {
		if (functionLanguage == FunctionLanguage.PYTHON) {
			return createFunctionDefinitionInternal(
				name,
				(UserDefinedFunction) PythonFunctionUtils.getPythonFunction(className, config));
		} else {
			return createJavaFunctionDefinition(name, className);
		}
	}

	private static FunctionDefinition createJavaFunctionDefinition(
			String name,
			String className) {
		Object func;
			func = Thread.currentThread().getContextClassLoader().loadClass(className).newInstance();
		

		return createFunctionDefinitionInternal(name, (UserDefinedFunction) func);
	}

	private static FunctionDefinition createFunctionDefinitionInternal(String name, UserDefinedFunction udf) {
		if (udf instanceof ScalarFunction || udf instanceof TableFunction) {
			// table and scalar function use the new type inference
			// once the other functions have been updated, this entire class will not be necessary
			// anymore and can be replaced with UserDefinedFunctionHelper.instantiateFunction
			return udf;
		} else if (udf instanceof AggregateFunction) {
			AggregateFunction a = (AggregateFunction) udf;

			return new AggregateFunctionDefinition(
				name,
				a,
				UserDefinedFunctionHelper.getReturnTypeOfAggregateFunction(a),
				UserDefinedFunctionHelper.getAccumulatorTypeOfAggregateFunction(a)
			);
		} else if (udf instanceof TableAggregateFunction) {
			TableAggregateFunction a = (TableAggregateFunction) udf;

			return new TableAggregateFunctionDefinition(
				name,
				a,
				UserDefinedFunctionHelper.getReturnTypeOfAggregateFunction(a),
				UserDefinedFunctionHelper.getAccumulatorTypeOfAggregateFunction(a)
			);
		} 
	}
}
