package org.apache.flink.table.runtime.runners.python.scalar.arrow;

import org.apache.flink.annotation.Internal;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.runtime.arrow.ArrowWriter;
import org.apache.flink.table.types.logical.RowType;

import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.util.Map;

/**
 * A {@link PythonFunctionRunner} used to execute Arrow Python {@link ScalarFunction}s.
 * It takes {@link RowData} as the input type.
 */
@Internal
public class RowDataArrowPythonScalarFunctionRunner extends AbstractArrowPythonScalarFunctionRunner<RowData> {

	public RowDataArrowPythonScalarFunctionRunner(
		String taskName,
		FnDataReceiver<byte[]> resultReceiver,
		PythonFunctionInfo[] scalarFunctions,
		PythonEnvironmentManager environmentManager,
		RowType inputType,
		RowType outputType,
		int maxBatchSize,
		Map<String, String> jobOptions,
		FlinkMetricContainer flinkMetricContainer) {
		super(taskName, resultReceiver, scalarFunctions, environmentManager, inputType, outputType, maxBatchSize, jobOptions, flinkMetricContainer);
	}

	@Override
	public ArrowWriter<RowData> createArrowWriter() {
		return ArrowUtils.createRowDataArrowWriter(root, getInputType());
	}
}
