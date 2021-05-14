

package org.apache.flink.table.runtime.generated;

import org.apache.flink.util.Collector;

/**
 * Describes a generated {@link Collector}.
 *
 * @param <C> type of collector
 */
public class GeneratedCollector<C extends Collector<?>> extends GeneratedClass<C> {


	/**
	 * Creates a GeneratedCollector.
	 *
	 * @param className class name of the generated Collector.
	 * @param code code of the generated Collector.
	 * @param references referenced objects of the generated Collector.
	 */
	public GeneratedCollector(String className, String code, Object[] references) {
		super(className, code, references);
	}
}
