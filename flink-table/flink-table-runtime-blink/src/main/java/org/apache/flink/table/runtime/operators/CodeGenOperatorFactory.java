package org.apache.flink.table.runtime.operators;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.runtime.generated.GeneratedClass;

/**
 * Stream operator factory for code gen operator.
 */
public class CodeGenOperatorFactory<OUT> extends AbstractStreamOperatorFactory<OUT> {

	private final GeneratedClass<? extends StreamOperator<OUT>> generatedClass;

	public CodeGenOperatorFactory(GeneratedClass<? extends StreamOperator<OUT>> generatedClass) {
		this.generatedClass = generatedClass;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends StreamOperator<OUT>> T createStreamOperator(StreamOperatorParameters<OUT> parameters) {
		return (T) generatedClass.newInstance(
			parameters.getContainingTask().getUserCodeClassLoader(),
			generatedClass.getReferences(),
			parameters.getContainingTask(),
			parameters.getStreamConfig(),
			parameters.getOutput(),
			processingTimeService);
	}

	@Override
	public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
		return generatedClass.getClass(classLoader);
	}

	public GeneratedClass<? extends StreamOperator<OUT>> getGeneratedClass() {
		return generatedClass;
	}
}
