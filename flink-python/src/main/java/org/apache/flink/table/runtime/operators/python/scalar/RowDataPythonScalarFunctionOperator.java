package org.apache.flink.table.runtime.operators.python.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.runners.python.scalar.RowDataPythonScalarFunctionRunner;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.types.logical.RowType;

import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.io.IOException;
import java.util.Map;

/**
 * The Python {@link ScalarFunction} operator for the blink planner.
 */
@Internal
public class RowDataPythonScalarFunctionOperator extends AbstractRowDataPythonScalarFunctionOperator {


	/**
	 * The TypeSerializer for udf execution results.
	 */
	private transient TypeSerializer<RowData> udfOutputTypeSerializer;

	public RowDataPythonScalarFunctionOperator(
		Configuration config,
		PythonFunctionInfo[] scalarFunctions,
		RowType inputType,
		RowType outputType,
		int[] udfInputOffsets,
		int[] forwardedFields) {
		super(config, scalarFunctions, inputType, outputType, udfInputOffsets, forwardedFields);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void open() throws Exception {
		super.open();
		udfOutputTypeSerializer = PythonTypeUtils.toBlinkTypeSerializer(userDefinedFunctionOutputType);
	}

	@Override
	@SuppressWarnings("ConstantConditions")
	public void emitResults() throws IOException {
		byte[] rawUdfResult;
		while ((rawUdfResult = userDefinedFunctionResultQueue.poll()) != null) {
			RowData input = forwardedInputQueue.poll();
			reuseJoinedRow.setRowKind(input.getRowKind());
			bais.setBuffer(rawUdfResult, 0, rawUdfResult.length);
			RowData udfResult = udfOutputTypeSerializer.deserialize(baisWrapper);
			rowDataWrapper.collect(reuseJoinedRow.replace(input, udfResult));
		}
	}

	@Override
	public PythonFunctionRunner<RowData> createPythonFunctionRunner(
			FnDataReceiver<byte[]> resultReceiver,
			PythonEnvironmentManager pythonEnvironmentManager,
			Map<String, String> jobOptions) {
		return new RowDataPythonScalarFunctionRunner(
			getRuntimeContext().getTaskName(),
			resultReceiver,
			scalarFunctions,
			pythonEnvironmentManager,
			userDefinedFunctionInputType,
			userDefinedFunctionOutputType,
			jobOptions,
			getFlinkMetricContainer());
	}
}
