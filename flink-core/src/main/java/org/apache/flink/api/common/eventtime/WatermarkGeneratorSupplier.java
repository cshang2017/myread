
package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.MetricGroup;

import java.io.Serializable;

/**
 * A supplier for {@link WatermarkGenerator WatermarkGenerators}. The supplier pattern is used to
 * avoid having to make {@link WatermarkGenerator} {@link Serializable} for use in API methods.
 *
 * <p>This interface is {@link Serializable} because the supplier may be shipped to workers during
 * distributed execution.
 */
@PublicEvolving
@FunctionalInterface
public interface WatermarkGeneratorSupplier<T> extends Serializable {

	/**
	 * Instantiates a {@link WatermarkGenerator}.
	 */
	WatermarkGenerator<T> createWatermarkGenerator(Context context);

	/**
	 * Additional information available to {@link #createWatermarkGenerator(Context)}. This can be
	 * access to {@link org.apache.flink.metrics.MetricGroup MetricGroups}, for example.
	 */
	interface Context {

		/**
		 * Returns the metric group for the context in which the created {@link WatermarkGenerator}
		 * is used.
		 *
		 * <p>Instances of this class can be used to register new metrics with Flink and to create
		 * a nested hierarchy based on the group names. See {@link MetricGroup} for more information
		 * for the metrics system.
		 *
		 * @see MetricGroup
		 */
		MetricGroup getMetricGroup();
	}
}
