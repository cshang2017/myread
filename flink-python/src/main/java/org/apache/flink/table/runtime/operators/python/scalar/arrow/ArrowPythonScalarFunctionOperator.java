package org.apache.flink.table.runtime.operators.python.scalar.arrow;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.runtime.operators.python.scalar.AbstractRowPythonScalarFunctionOperator;
import org.apache.flink.table.runtime.runners.python.scalar.arrow.ArrowPythonScalarFunctionRunner;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.io.IOException;
import java.util.Map;

/**
 * Arrow Python {@link ScalarFunction} operator for the old planner.
 */
@Internal
public class ArrowPythonScalarFunctionOperator extends AbstractRowPythonScalarFunctionOperator {

	/**
	 * Allocator which is used for byte buffer allocation.
	 */
	private transient BufferAllocator allocator;

	/**
	 * Reader which is responsible for deserialize the Arrow format data to the Flink rows.
	 */
	private transient ArrowReader<Row> arrowReader;

	/**
	 * Reader which is responsible for convert the execution result from
	 * byte array to arrow format.
	 */
	private transient ArrowStreamReader reader;

	public ArrowPythonScalarFunctionOperator(
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
	public PythonFunctionRunner<Row> createPythonFunctionRunner(
		FnDataReceiver<byte[]> resultReceiver,
		PythonEnvironmentManager pythonEnvironmentManager,
		Map<String, String> jobOptions) {
		return new ArrowPythonScalarFunctionRunner(
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
				arrowReader = ArrowUtils.createRowArrowReader(root, outputType);
			}
			for (int i = 0; i < root.getRowCount(); i++) {
				CRow input = forwardedInputQueue.poll();
				cRowWrapper.setChange(input.change());
				cRowWrapper.collect(Row.join(input.row(), arrowReader.read(i)));
			}
		}
	}
}
