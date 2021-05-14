package org.apache.flink.runtime.jobgraph;

import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.operators.util.TaskConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A task vertex that runs an initialization and a finalization on the master. If necessary, it tries
 * to deserialize input and output formats, and initialize and finalize them on master.
 */
public class InputOutputFormatVertex extends JobVertex {


	private final Map<OperatorID, String> formatDescriptions = new HashMap<>();

	public InputOutputFormatVertex(String name) {
		super(name);
	}

	public InputOutputFormatVertex(
		String name,
		JobVertexID id,
		List<OperatorIDPair> operatorIDPairs) {

		super(name, id, operatorIDPairs);
	}

	@Override
	public void initializeOnMaster(ClassLoader loader) throws Exception {
		final InputOutputFormatContainer formatContainer = initInputOutputformatContainer(loader);

		final ClassLoader original = Thread.currentThread().getContextClassLoader();
		try {
			// set user classloader before calling user code
			Thread.currentThread().setContextClassLoader(loader);

			// configure the input format and setup input splits
			Map<OperatorID, UserCodeWrapper<? extends InputFormat<?, ?>>> inputFormats = formatContainer.getInputFormats();
			for (Map.Entry<OperatorID, UserCodeWrapper<? extends InputFormat<?, ?>>> entry : inputFormats.entrySet()) {
				final InputFormat<?, ?> inputFormat;

					inputFormat = entry.getValue().getUserCodeObject();
					inputFormat.configure(formatContainer.getParameters(entry.getKey()));

				setInputSplitSource(inputFormat);
			}

			// configure input formats and invoke initializeGlobal()
			Map<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> outputFormats = formatContainer.getOutputFormats();
			for (Map.Entry<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> entry : outputFormats.entrySet()) {
				final OutputFormat<?> outputFormat;

					outputFormat = entry.getValue().getUserCodeObject();
					outputFormat.configure(formatContainer.getParameters(entry.getKey()));

				if (outputFormat instanceof InitializeOnMaster) {
					((InitializeOnMaster) outputFormat).initializeGlobal(getParallelism());
				}
			}

		} finally {
			// restore original classloader
			Thread.currentThread().setContextClassLoader(original);
		}
	}

	@Override
	public void finalizeOnMaster(ClassLoader loader) throws Exception {
		final InputOutputFormatContainer formatContainer = initInputOutputformatContainer(loader);

		final ClassLoader original = Thread.currentThread().getContextClassLoader();
		try {
			// set user classloader before calling user code
			Thread.currentThread().setContextClassLoader(loader);

			// configure input formats and invoke finalizeGlobal()
			Map<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> outputFormats = formatContainer.getOutputFormats();
			for (Map.Entry<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> entry : outputFormats.entrySet()) {
				final OutputFormat<?> outputFormat;

					outputFormat = entry.getValue().getUserCodeObject();
					outputFormat.configure(formatContainer.getParameters(entry.getKey()));

				if (outputFormat instanceof FinalizeOnMaster) {
					((FinalizeOnMaster) outputFormat).finalizeGlobal(getParallelism());
				}
			}

		} finally {
			// restore original classloader
			Thread.currentThread().setContextClassLoader(original);
		}
	}

	public String getFormatDescription(OperatorID operatorID) {
		return formatDescriptions.get(operatorID);
	}

	public void setFormatDescription(OperatorID operatorID, String formatDescription) {
		formatDescriptions.put(checkNotNull(operatorID), formatDescription);
	}

	private InputOutputFormatContainer initInputOutputformatContainer(ClassLoader classLoader) throws Exception {
			return new InputOutputFormatContainer(new TaskConfig(getConfiguration()), classLoader);
	}
}
