package org.apache.flink.cep;

import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Utility class for complex event processing.
 *
 * <p>Methods which transform a {@link DataStream} into a {@link PatternStream} to do CEP.
 */
public class CEP {
	/**
	 * Creates a {@link PatternStream} from an input data stream and a pattern.
	 *
	 * @param input DataStream containing the input events
	 * @param pattern Pattern specification which shall be detected
	 * @param <T> Type of the input events
	 * @return Resulting pattern stream
	 */
	public static <T> PatternStream<T> pattern(DataStream<T> input, Pattern<T, ?> pattern) {
		return new PatternStream<>(input, pattern);
	}

	/**
	 * Creates a {@link PatternStream} from an input data stream and a pattern.
	 *
	 * @param input DataStream containing the input events
	 * @param pattern Pattern specification which shall be detected
	 * @param comparator Comparator to sort events with equal timestamps
	 * @param <T> Type of the input events
	 * @return Resulting pattern stream
	 */
	public static <T> PatternStream<T> pattern(
			DataStream<T> input,
			Pattern<T, ?> pattern,
			EventComparator<T> comparator) {
		final PatternStream<T> stream = new PatternStream<>(input, pattern);
		return stream.withComparator(comparator);
	}
}
