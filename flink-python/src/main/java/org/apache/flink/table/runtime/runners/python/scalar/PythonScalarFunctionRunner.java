package org.apache.flink.table.runtime.runners.python.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.util.Map;

/**
 * A {@link PythonFunctionRunner} used to execute Python {@link ScalarFunction}s.
 * It takes {@link Row} as the input and outputs a byte array.
 */
@Internal
public class PythonScalarFunctionRunner extends AbstractGeneralPythonScalarFunctionRunner<Row> {

	public PythonScalarFunctionRunner(
		String taskName,
		FnDataReceiver<byte[]> resultReceiver,
		PythonFunctionInfo[] scalarFunctions,
		PythonEnvironmentManager environmentManager,
		RowType inputType,
		RowType outputType,
		Map<String, String> jobOptions,
		FlinkMetricContainer flinkMetricContainer) {
		super(taskName, resultReceiver, scalarFunctions, environmentManager, inputType, outputType, jobOptions, flinkMetricContainer);
	}

	@Override
	@SuppressWarnings("unchecked")
	public RowSerializer getInputTypeSerializer() {
		return (RowSerializer) PythonTypeUtils.toFlinkTypeSerializer(getInputType());
	}
}
