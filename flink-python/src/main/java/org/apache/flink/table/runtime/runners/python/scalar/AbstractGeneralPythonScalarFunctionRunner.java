package org.apache.flink.table.runtime.runners.python.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;

import java.util.Map;

/**
 * Abstract {@link PythonFunctionRunner} used to execute Python {@link ScalarFunction}s.
 *
 * @param <IN> Type of the input elements.
 */
@Internal
public abstract class AbstractGeneralPythonScalarFunctionRunner<IN> extends AbstractPythonScalarFunctionRunner<IN> {

	private static final String SCALAR_FUNCTION_SCHEMA_CODER_URN = "flink:coder:schema:scalar_function:v1";

	/**
	 * The TypeSerializer for input elements.
	 */
	private transient TypeSerializer<IN> inputTypeSerializer;

	public AbstractGeneralPythonScalarFunctionRunner(
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
	public void open() throws Exception {
		super.open();
		inputTypeSerializer = getInputTypeSerializer();
	}

	@Override
	public void processElement(IN element) {
			baos.reset();
			inputTypeSerializer.serialize(element, baosWrapper);
			// TODO: support to use ValueOnlyWindowedValueCoder for better performance.
			// Currently, FullWindowedValueCoder has to be used in Beam's portability framework.
			mainInputReceiver.accept(WindowedValue.valueInGlobalWindow(baos.toByteArray()));
	}

	@Override
	public OutputReceiverFactory createOutputReceiverFactory() {
		return new OutputReceiverFactory() {

			// the input value type is always byte array
			@SuppressWarnings("unchecked")
			@Override
			public FnDataReceiver<WindowedValue<byte[]>> create(String pCollectionId) {
				return input -> resultReceiver.accept(input.getValue());
			}
		};
	}

	@Override
	public String getInputOutputCoderUrn() {
		return SCALAR_FUNCTION_SCHEMA_CODER_URN;
	}

	/**
	 * Returns the TypeSerializer for input elements.
	 */
	public abstract TypeSerializer<IN> getInputTypeSerializer();
}
