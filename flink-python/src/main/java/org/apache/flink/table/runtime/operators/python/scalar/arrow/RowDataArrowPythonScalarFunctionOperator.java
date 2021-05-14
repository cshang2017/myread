package org.apache.flink.table.runtime.operators.python.scalar.arrow;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.runtime.operators.python.scalar.AbstractRowDataPythonScalarFunctionOperator;
import org.apache.flink.table.runtime.runners.python.scalar.arrow.RowDataArrowPythonScalarFunctionRunner;
import org.apache.flink.table.types.logical.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.io.IOException;
import java.util.Map;

/**
 * Arrow Python {@link ScalarFunction} operator for the blink planner.
 */
@Internal
public class RowDataArrowPythonScalarFunctionOperator extends AbstractRowDataPythonScalarFunctionOperator {


	/**
	 * Allocator which is used for byte buffer allocation.
	 */
	private transient BufferAllocator allocator;

	/**
	 * Reader which is responsible for deserialize the Arrow format data to the Flink rows.
	 */
	private transient ArrowReader<RowData> arrowReader;

	/**
	 * Reader which is responsible for convert the execution result from
	 * byte array to arrow format.
	 */
	private transient ArrowStreamReader reader;

	public RowDataArrowPythonScalarFunctionOperator(
		Configuration config,
		PythonFunctionInfo[] scalarFunctions,
		RowType inputType,
		RowType outputType,
		int[] udfInputOffsets,
		int[] forwardedFields) {
		super(config, scalarFunctions, inputType, outputType, udfInputOffsets, forwardedFields);
	}

	@Override
	public void open() throws Exception {
		super.open();
		allocator = ArrowUtils.getRootAllocator().newChildAllocator("reader", 0, Long.MAX_VALUE);
		reader = new ArrowStreamReader(bais, allocator);
	}

	@Override
	public void close() throws Exception {
		try {
			super.close();
		} finally {
			reader.close();
			allocator.close();
		}
	}

	@Override
	public PythonFunctionRunner<RowData> createPythonFunctionRunner(
		FnDataReceiver<byte[]> resultReceiver,
		PythonEnvironmentManager pythonEnvironmentManager,
		Map<String, String> jobOptions) {
		return new RowDataArrowPythonScalarFunctionRunner(
			getRuntimeContext().getTaskName(),
			resultReceiver,
			scalarFunctions,
			pythonEnvironmentManager,
			userDefinedFunctionInputType,
			userDefinedFunctionOutputType,
			getPythonConfig().getMaxArrowBatchSize(),
			jobOptions,
			getFlinkMetricContainer());
	}

	@Override
	@SuppressWarnings("ConstantConditions")
	public void emitResults() throws IOException {
		byte[] udfResult;
		while ((udfResult = userDefinedFunctionResultQueue.poll()) != null) {
			bais.setBuffer(udfResult, 0, udfResult.length);
			reader.loadNextBatch();
			VectorSchemaRoot root = reader.getVectorSchemaRoot();
			if (arrowReader == null) {
				arrowReader = ArrowUtils.createRowDataArrowReader(root, outputType);
			}
			for (int i = 0; i < root.getRowCount(); i++) {
				RowData input = forwardedInputQueue.poll();
				reuseJoinedRow.setRowKind(input.getRowKind());
				rowDataWrapper.collect(reuseJoinedRow.replace(input, arrowReader.read(i)));
			}
		}
	}
}
