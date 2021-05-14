package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceAware;

/**
 * Base class for all stream operator factories. It implements some common methods and the
 * {@link ProcessingTimeServiceAware} interface which enables stream operators to access
 * {@link ProcessingTimeService}.
 */
@Experimental
public abstract class AbstractStreamOperatorFactory<OUT> implements StreamOperatorFactory<OUT>, ProcessingTimeServiceAware {

	protected ChainingStrategy chainingStrategy = ChainingStrategy.ALWAYS;

	protected transient ProcessingTimeService processingTimeService;

	@Override
	public void setChainingStrategy(ChainingStrategy strategy) {
		this.chainingStrategy = strategy;
	}

	@Override
	public ChainingStrategy getChainingStrategy() {
		return chainingStrategy;
	}

	@Override
	public void setProcessingTimeService(ProcessingTimeService processingTimeService) {
		this.processingTimeService = processingTimeService;
	}
}
