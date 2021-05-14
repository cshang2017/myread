package org.apache.flink.table.runtime.operators.wmassigners;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedWatermarkGenerator;
import org.apache.flink.table.runtime.generated.WatermarkGenerator;

public class WatermarkAssignerOperatorFactory extends AbstractStreamOperatorFactory<RowData>
	implements OneInputStreamOperatorFactory<RowData, RowData> {

	private final int rowtimeFieldIndex;
	private final long idleTimeout;
	private final GeneratedWatermarkGenerator generatedWatermarkGenerator;

	public WatermarkAssignerOperatorFactory(
			int rowtimeFieldIndex,
			long idleTimeout,
			GeneratedWatermarkGenerator generatedWatermarkGenerator) {
		this.rowtimeFieldIndex = rowtimeFieldIndex;
		this.idleTimeout = idleTimeout;
		this.generatedWatermarkGenerator = generatedWatermarkGenerator;
	}

	@SuppressWarnings("unchecked")
	@Override
	public StreamOperator createStreamOperator(StreamOperatorParameters initializer) {
		WatermarkGenerator watermarkGenerator = generatedWatermarkGenerator.newInstance(
			initializer.getContainingTask().getUserCodeClassLoader());

		WatermarkAssignerOperator operator = new WatermarkAssignerOperator(
			rowtimeFieldIndex,
			watermarkGenerator,
			idleTimeout,
			processingTimeService);

		operator.setup(
			initializer.getContainingTask(),
			initializer.getStreamConfig(),
			initializer.getOutput());
			
		return operator;
	}

	@Override
	public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
		return WatermarkAssignerOperator.class;
	}
}
