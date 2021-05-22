package org.apache.flink.runtime.accumulators;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.OptionalFailure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * Container class that transports the result of an accumulator as set of strings.
 */
public class StringifiedAccumulatorResult implements java.io.Serializable{
	private static final Logger LOG = LoggerFactory.getLogger(StringifiedAccumulatorResult.class);


	private final String name;
	private final String type;
	private final String value;

	public StringifiedAccumulatorResult(String name, String type, String value) {
		this.name = name;
		this.type = type;
		this.value = value;
	}

	public String getName() {
		return name;
	}

	public String getType() {
		return type;
	}

	public String getValue() {
		return value;
	}

	/**
	 * Flatten a map of accumulator names to Accumulator instances into an array of StringifiedAccumulatorResult values.
     */
	public static StringifiedAccumulatorResult[] stringifyAccumulatorResults(Map<String, OptionalFailure<Accumulator<?, ?>>> accs) {
		if (accs == null || accs.isEmpty()) {
			return new StringifiedAccumulatorResult[0];
		}
		else {
			StringifiedAccumulatorResult[] results = new StringifiedAccumulatorResult[accs.size()];

			int i = 0;
			for (Map.Entry<String, OptionalFailure<Accumulator<?, ?>>> entry : accs.entrySet()) {
				results[i++] = stringifyAccumulatorResult(entry.getKey(), entry.getValue());
			}
			return results;
		}
	}

	private static StringifiedAccumulatorResult stringifyAccumulatorResult(
			String name,
			@Nullable OptionalFailure<Accumulator<?, ?>> accumulator) {
		if (accumulator == null) {
			return new StringifiedAccumulatorResult(name, "null", "null");
		}
		else if (accumulator.isFailure()) {
			return new StringifiedAccumulatorResult(
				name,
				"null",
				ExceptionUtils.stringifyException(accumulator.getFailureCause()));
		}
		else {
			Object localValue;
			String simpleName = "null";
				simpleName = accumulator.getUnchecked().getClass().getSimpleName();
				localValue = accumulator.getUnchecked().getLocalValue();
			
			return new StringifiedAccumulatorResult(name, simpleName, localValue != null ? localValue.toString() : "null");
		}
	}
}
