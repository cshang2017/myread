

package org.apache.flink.table.runtime.generated;

import org.apache.flink.streaming.api.operators.StreamOperator;

/**
 * Describes a generated {@link StreamOperator}.
 *
 * @param <C> type of StreamOperator
 */
public class GeneratedOperator<C extends StreamOperator<?>> extends GeneratedClass<C> {


	/**
	 * Creates a GeneratedOperator.
	 *
	 * @param className class name of the generated StreamOperator.
	 * @param code code of the generated StreamOperator.
	 * @param references referenced objects of the generated StreamOperator.
	 */
	public GeneratedOperator(String className, String code, Object[] references) {
		super(className, code, references);
	}
}
