package org.apache.flink.table.runtime.runners.python.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.runners.python.AbstractPythonStatelessFunctionRunner;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.util.Map;

/**
 * Abstract {@link PythonFunctionRunner} used to execute Python {@link ScalarFunction}s.
 *
 * @param <IN> Type of the input elements.
 */
@Internal
public abstract class AbstractPythonScalarFunctionRunner<IN> extends AbstractPythonStatelessFunctionRunner<IN> {

	private static final String SCALAR_FUNCTION_URN = "flink:transform:scalar_function:v1";

	private final PythonFunctionInfo[] scalarFunctions;

	public AbstractPythonScalarFunctionRunner(
		String taskName,
		FnDataReceiver<byte[]> resultReceiver,
		PythonFunctionInfo[] scalarFunctions,
		PythonEnvironmentManager environmentManager,
		RowType inputType,
		RowType outputType,
		Map<String, String> jobOptions,
		FlinkMetricContainer flinkMetricContainer) {
		super(taskName, resultReceiver, environmentManager, inputType, outputType, SCALAR_FUNCTION_URN, jobOptions, flinkMetricContainer);
		this.scalarFunctions = Preconditions.checkNotNull(scalarFunctions);
	}

	/**
	 * Gets the proto representation of the Python user-defined functions to be executed.
	 */
	@VisibleForTesting
	public FlinkFnApi.UserDefinedFunctions getUserDefinedFunctionsProto() {
		FlinkFnApi.UserDefinedFunctions.Builder builder = FlinkFnApi.UserDefinedFunctions.newBuilder();
		// add udf proto
		for (PythonFunctionInfo pythonFunctionInfo : scalarFunctions) {
			builder.addUdfs(getUserDefinedFunctionProto(pythonFunctionInfo));
		}
		builder.setMetricEnabled(flinkMetricContainer != null);
		return builder.build();
	}
}
