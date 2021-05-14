package org.apache.flink.table.runtime.runners.python.scalar.arrow;

import org.apache.flink.annotation.Internal;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.runtime.arrow.ArrowWriter;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.util.Map;

/**
 * A {@link PythonFunctionRunner} used to execute Arrow Python {@link ScalarFunction}s.
 * It takes {@link Row} as the input type.
 */
@Internal
public class ArrowPythonScalarFunctionRunner extends AbstractArrowPythonScalarFunctionRunner<Row> {

	public ArrowPythonScalarFunctionRunner(
		String taskName,
		FnDataReceiver<byte[]> resultReceiver,
		PythonFunctionInfo[] scalarFunctions,
		PythonEnvironmentManager environmentManager,
		RowType inputType,
		RowType outputType,
		int maxArrowBatchSize,
		Map<String, String> jobOptions,
		FlinkMetricContainer flinkMetricContainer) {
		super(taskName, resultReceiver, scalarFunctions, environmentManager, inputType, outputType, maxArrowBatchSize, jobOptions, flinkMetricContainer);
	}

	@Override
	public ArrowWriter<Row> createArrowWriter() {
		return ArrowUtils.createRowArrowWriter(root, getInputType());
	}
}
