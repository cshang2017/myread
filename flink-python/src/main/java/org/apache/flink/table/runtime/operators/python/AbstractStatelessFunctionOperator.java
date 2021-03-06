package org.apache.flink.table.runtime.operators.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.streaming.api.operators.python.AbstractPythonFunctionOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * Base class for all stream operators to execute Python Stateless Functions.
 *
 * @param <IN>    Type of the input elements.
 * @param <OUT>   Type of the output elements.
 * @param <UDFIN> Type of the UDF input type.
 */
@Internal
public abstract class AbstractStatelessFunctionOperator<IN, OUT, UDFIN>
	extends AbstractPythonFunctionOperator<IN, OUT> {

	protected final RowType inputType;
	protected final RowType outputType;

	protected final int[] userDefinedFunctionInputOffsets;

	private final Map<String, String> jobOptions;

	protected transient RowType userDefinedFunctionInputType;
	protected transient RowType userDefinedFunctionOutputType;

	/**
	 * The queue holding the input elements for which the execution results have not been received.
	 */
	protected transient LinkedBlockingQueue<IN> forwardedInputQueue;

	/**
	 * The queue holding the user-defined function execution results. The execution results
	 * are in the same order as the input elements.
	 */
	protected transient LinkedBlockingQueue<byte[]> userDefinedFunctionResultQueue;

	/**
	 * Reusable InputStream used to holding the execution results to be deserialized.
	 */
	protected transient ByteArrayInputStreamWithPos bais;

	/**
	 * InputStream Wrapper.
	 */
	protected transient DataInputViewStreamWrapper baisWrapper;

	public AbstractStatelessFunctionOperator(
		Configuration config,
		RowType inputType,
		RowType outputType,
		int[] userDefinedFunctionInputOffsets) {
		super(config);
		this.inputType = Preconditions.checkNotNull(inputType);
		this.outputType = Preconditions.checkNotNull(outputType);
		this.userDefinedFunctionInputOffsets = Preconditions.checkNotNull(userDefinedFunctionInputOffsets);
		this.jobOptions = buildJobOptions(config);
	}

	@Override
	public void open() throws Exception {
		forwardedInputQueue = new LinkedBlockingQueue<>();
		userDefinedFunctionResultQueue = new LinkedBlockingQueue<>();
		userDefinedFunctionInputType = new RowType(
			Arrays.stream(userDefinedFunctionInputOffsets)
				.mapToObj(i -> inputType.getFields().get(i))
				.collect(Collectors.toList()));
		bais = new ByteArrayInputStreamWithPos();
		baisWrapper = new DataInputViewStreamWrapper(bais);
		super.open();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		bufferInput(element.getValue());
		super.processElement(element);
		emitResults();
	}

	@Override
	public PythonFunctionRunner<IN> createPythonFunctionRunner() throws IOException {
		final FnDataReceiver<byte[]> userDefinedFunctionResultReceiver = input -> {
			// handover to queue, do not block the result receiver thread
			userDefinedFunctionResultQueue.put(input);
		};

		return new ProjectUdfInputPythonScalarFunctionRunner(
			createPythonFunctionRunner(
				userDefinedFunctionResultReceiver,
				createPythonEnvironmentManager(),
				jobOptions));
	}

	/**
	 * Buffers the specified input, it will be used to construct
	 * the operator result together with the user-defined function execution result.
	 */
	public abstract void bufferInput(IN input);

	public abstract UDFIN getFunctionInput(IN element);

	public abstract PythonFunctionRunner<UDFIN> createPythonFunctionRunner(
		FnDataReceiver<byte[]> resultReceiver,
		PythonEnvironmentManager pythonEnvironmentManager,
		Map<String, String> jobOptions);

	private Map<String, String> buildJobOptions(Configuration config) {
		Map<String, String> jobOptions = new HashMap<>();
		if (config.containsKey("table.exec.timezone")) {
			jobOptions.put("table.exec.timezone", config.getString("table.exec.timezone", null));
		}
		return jobOptions;
	}

	private class ProjectUdfInputPythonScalarFunctionRunner implements PythonFunctionRunner<IN> {

		private final PythonFunctionRunner<UDFIN> pythonFunctionRunner;

		ProjectUdfInputPythonScalarFunctionRunner(PythonFunctionRunner<UDFIN> pythonFunctionRunner) {
			this.pythonFunctionRunner = pythonFunctionRunner;
		}

		@Override
		public void open() throws Exception {
			pythonFunctionRunner.open();
		}

		@Override
		public void close() throws Exception {
			pythonFunctionRunner.close();
		}

		@Override
		public void startBundle() throws Exception {
			pythonFunctionRunner.startBundle();
		}

		@Override
		public void finishBundle() throws Exception {
			pythonFunctionRunner.finishBundle();
		}

		@Override
		public void processElement(IN element) throws Exception {
			pythonFunctionRunner.processElement(getFunctionInput(element));
		}
	}

	/**
	 * The collector is used to convert a {@link Row} to a {@link CRow}.
	 */
	public static class StreamRecordCRowWrappingCollector implements Collector<Row> {

		private final Collector<StreamRecord<CRow>> out;
		private final CRow reuseCRow = new CRow();

		/**
		 * For Table API & SQL jobs, the timestamp field is not used.
		 */
		private final StreamRecord<CRow> reuseStreamRecord = new StreamRecord<>(reuseCRow);

		public StreamRecordCRowWrappingCollector(Collector<StreamRecord<CRow>> out) {
			this.out = out;
		}

		public void setChange(boolean change) {
			this.reuseCRow.change_$eq(change);
		}

		@Override
		public void collect(Row record) {
			reuseCRow.row_$eq(record);
			out.collect(reuseStreamRecord);
		}

		@Override
		public void close() {
			out.close();
		}
	}

	/**
	 * The collector is used to convert a {@link RowData} to a {@link StreamRecord}.
	 */
	public static class StreamRecordRowDataWrappingCollector implements Collector<RowData> {

		private final Collector<StreamRecord<RowData>> out;

		/**
		 * For Table API & SQL jobs, the timestamp field is not used.
		 */
		private final StreamRecord<RowData> reuseStreamRecord = new StreamRecord<>(null);

		public StreamRecordRowDataWrappingCollector(Collector<StreamRecord<RowData>> out) {
			this.out = out;
		}

		@Override
		public void collect(RowData record) {
			out.collect(reuseStreamRecord.replace(record));
		}

		@Override
		public void close() {
			out.close();
		}
	}
}
