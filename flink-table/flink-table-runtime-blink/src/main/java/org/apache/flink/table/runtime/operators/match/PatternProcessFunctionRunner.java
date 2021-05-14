

package org.apache.flink.table.runtime.operators.match;

import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * A {@link PatternProcessFunction} wrapper to delegate invocation to the code generated
 * {@link PatternProcessFunction}.
 */
public class PatternProcessFunctionRunner extends PatternProcessFunction<RowData, RowData> {

	private final GeneratedFunction<PatternProcessFunction<RowData, RowData>> generatedFunction;
	private transient PatternProcessFunction<RowData, RowData> function;

	public PatternProcessFunctionRunner(GeneratedFunction<PatternProcessFunction<RowData, RowData>> generatedFunction) {
		this.generatedFunction = generatedFunction;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		this.function = generatedFunction.newInstance(getRuntimeContext().getUserCodeClassLoader());
		FunctionUtils.setFunctionRuntimeContext(function, getRuntimeContext());
		FunctionUtils.openFunction(function, parameters);
	}

	@Override
	public void processMatch(Map<String, List<RowData>> match, Context ctx, Collector<RowData> out) throws Exception {
		function.processMatch(match, ctx, out);
	}

	@Override
	public void close() throws Exception {
		FunctionUtils.closeFunction(function);
	}
}
