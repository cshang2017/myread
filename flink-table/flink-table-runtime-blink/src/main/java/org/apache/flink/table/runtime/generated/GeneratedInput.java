package org.apache.flink.table.runtime.generated;

import org.apache.flink.api.common.io.InputFormat;

/**
 * Describes a generated {@link InputFormat}.
 *
 * @param <F> type of Function
 */
public class GeneratedInput<F extends InputFormat<?, ?>> extends GeneratedClass<F> {


	/**
	 * Creates a GeneratedInput.
	 *
	 * @param className class name of the generated Function.
	 * @param code code of the generated Function.
	 * @param references referenced objects of the generated Function.
	 */
	public GeneratedInput(String className, String code, Object[] references) {
		super(className, code, references);
	}
}
