package org.apache.flink.table.runtime.generated;

import org.apache.flink.api.common.functions.Function;

/**
 * Describes a generated {@link Function}.
 *
 * @param <F> type of Function
 */
public class GeneratedFunction<F extends Function> extends GeneratedClass<F> {


	/**
	 * Creates a GeneratedFunction.
	 *
	 * @param className class name of the generated Function.
	 * @param code code of the generated Function.
	 * @param references referenced objects of the generated Function.
	 */
	public GeneratedFunction(String className, String code, Object[] references) {
		super(className, code, references);
	}
}
