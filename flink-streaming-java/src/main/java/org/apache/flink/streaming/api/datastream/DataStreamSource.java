package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.operators.util.OperatorValidationUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;

/**
 * The DataStreamSource represents the starting point of a DataStream.
 *
 * @param <T> Type of the elements in the DataStream created from the this source.
 */
@Public
public class DataStreamSource<T> extends SingleOutputStreamOperator<T> {

	boolean isParallel;

	/**
	 * The constructor used to create legacy sources.
	 */
	public DataStreamSource(
			StreamExecutionEnvironment environment,
			TypeInformation<T> outTypeInfo,
			StreamSource<T, ?> operator,
			boolean isParallel,
			String sourceName) {
		super(environment, new LegacySourceTransformation<>(sourceName, operator, outTypeInfo, environment.getParallelism()));

		this.isParallel = isParallel;
		if (!isParallel) {
			setParallelism(1);
		}
	}

	/**
	 * Constructor for "deep" sources that manually set up (one or more) custom configured complex operators.
	 */
	public DataStreamSource(SingleOutputStreamOperator<T> operator) {
		super(operator.environment, operator.getTransformation());
		this.isParallel = true;
	}

	/**
	 * Constructor for new Sources (FLIP-27).
	 */
	public DataStreamSource(
			StreamExecutionEnvironment environment,
			Source<T, ?, ?> source,
			WatermarkStrategy<T> timestampsAndWatermarks,
			TypeInformation<T> outTypeInfo,
			String sourceName) {
		super(environment,
				new SourceTransformation<>(
						sourceName,
						new SourceOperatorFactory<>(source, timestampsAndWatermarks),
						outTypeInfo,
						environment.getParallelism()));
	}

	@Override
	public DataStreamSource<T> setParallelism(int parallelism) {
		OperatorValidationUtils.validateParallelism(parallelism, isParallel);
		super.setParallelism(parallelism);
		return this;
	}
}
