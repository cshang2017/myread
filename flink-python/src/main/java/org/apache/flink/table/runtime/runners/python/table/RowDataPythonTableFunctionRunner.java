package org.apache.flink.table.runtime.runners.python.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;

import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.util.Map;

/**
 * A {@link PythonFunctionRunner} used to execute Python {@link TableFunction}.
 * It takes {@link RowData} as the input and outputs a byte array.
 */
@Internal
public class RowDataPythonTableFunctionRunner extends AbstractPythonTableFunctionRunner<RowData> {
	public RowDataPythonTableFunctionRunner(
		String taskName,
		FnDataReceiver<byte[]> resultReceiver,
		PythonFunctionInfo tableFunction,
		PythonEnvironmentManager environmentManager,
		RowType inputType,
		RowType outputType,
		Map<String, String> jobOptions,
		FlinkMetricContainer flinkMetricContainer) {
		super(taskName, resultReceiver, tableFunction, environmentManager, inputType, outputType, jobOptions, flinkMetricContainer);
	}

	@Override
	public RowDataSerializer getInputTypeSerializer() {
		return (RowDataSerializer) PythonTypeUtils.toBlinkTypeSerializer(getInputType());
	}
}
