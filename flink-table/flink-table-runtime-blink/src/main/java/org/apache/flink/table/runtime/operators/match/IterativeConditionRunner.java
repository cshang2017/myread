package org.apache.flink.table.runtime.operators.match;

import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.cep.pattern.conditions.RichIterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;

/**
 * A {@link RichIterativeCondition} wrapper to delegate invocation to the code generated
 * {@link RichIterativeCondition}.
 */
public class IterativeConditionRunner extends RichIterativeCondition<RowData> {
	private static final long serialVersionUID = 1L;

	private final GeneratedFunction<RichIterativeCondition<RowData>> generatedFunction;
	private transient RichIterativeCondition<RowData> function;

	public IterativeConditionRunner(GeneratedFunction<RichIterativeCondition<RowData>> generatedFunction) {
		this.generatedFunction = generatedFunction;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		this.function = generatedFunction.newInstance(getRuntimeContext().getUserCodeClassLoader());
		FunctionUtils.setFunctionRuntimeContext(function, getRuntimeContext());
		FunctionUtils.openFunction(function, parameters);
	}

	@Override
	public boolean filter(RowData value, Context<RowData> ctx) throws Exception {
		return function.filter(value, ctx);
	}

	@Override
	public void close() throws Exception {
		FunctionUtils.closeFunction(function);
	}
}
