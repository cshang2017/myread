package org.apache.flink.table.factories;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.module.Module;

import java.util.Map;

/**
 * A factory to create configured module instances based on string-based properties. See
 * also {@link TableFactory} for more information.
 */
@PublicEvolving
public interface ModuleFactory extends TableFactory {

	/**
	 * Creates and configures a {@link Module} using the given properties.
	 *
	 * @param properties normalized properties describing a module.
	 * @return the configured module.
	 */
	Module createModule(Map<String, String> properties);
}
