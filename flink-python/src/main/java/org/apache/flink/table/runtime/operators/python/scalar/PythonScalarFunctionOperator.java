package org.apache.flink.table.runtime.operators.python.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.runners.python.scalar.PythonScalarFunctionRunner;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.io.IOException;
import java.util.Map;

/**
 * The Python {@link ScalarFunction} operator for the legacy planner.
 */
@Internal
public class PythonScalarFunctionOperator extends AbstractRowPythonScalarFunctionOperator {


	/**
	 * The TypeSerializer for udf execution results.
	 */
	private transient TypeSerializer<Row> udfOutputTypeSerializer;

	public PythonScalarFunctionOperator(
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
		udfOutputTypeSerializer = PythonTypeUtils.toFlinkTypeSerializer(userDefinedFunctionOutputType);
	}

	@Override
	public void emitResults() {
		byte[] rawUdfResult;
		while ((rawUdfResult = userDefinedFunctionResultQueue.poll()) != null) {
			CRow input = forwardedInputQueue.poll();
			cRowWrapper.setChange(input.change());
			bais.setBuffer(rawUdfResult, 0, rawUdfResult.length);
			Row udfResult = udfOutputTypeSerializer.deserialize(baisWrapper);
			cRowWrapper.collect(Row.join(input.row(), udfResult));
		}
	}

	@Override
	public PythonFunctionRunner<Row> createPythonFunctionRunner(
			FnDataReceiver<byte[]> resultReceiver,
			PythonEnvironmentManager pythonEnvironmentManager,
			Map<String, String> jobOptions) {
		return new PythonScalarFunctionRunner(
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
