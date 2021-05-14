package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.ChainingStrategy;

/**
 * A {@link Transformation} that creates a physical operation. It enables setting {@link ChainingStrategy}.
 *
 * @param <T> The type of the elements that result from this {@code Transformation}
 * @see Transformation
 */
@Internal
public abstract class PhysicalTransformation<T> extends Transformation<T> {

	/**
	 * Creates a new {@code Transformation} with the given name, output type and parallelism.
	 *
	 * @param name The name of the {@code Transformation}, this will be shown in Visualizations and the Log
	 * @param outputType The output type of this {@code Transformation}
	 * @param parallelism The parallelism of this {@code Transformation}
	 */
	PhysicalTransformation(
		String name,
		TypeInformation<T> outputType,
		int parallelism) {
		super(name, outputType, parallelism);
	}

	/**
	 * Sets the chaining strategy of this {@code Transformation}.
	 */
	public abstract void setChainingStrategy(ChainingStrategy strategy);
}
