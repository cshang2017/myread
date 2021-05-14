

package org.apache.flink.table.module;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.functions.FunctionDefinition;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * Modules define a set of metadata, including functions, user defined types, operators, rules, etc.
 * Metadata from modules are regarded as built-in or system metadata that users can take advantages of.
 */
@PublicEvolving
public interface Module {

	/**
	 * List names of all functions in this module.
	 *
	 * @return a set of function names
	 */
	default Set<String> listFunctions() {
		return Collections.emptySet();
	}

	/**
	 * Get an optional of {@link FunctionDefinition} by a give name.
	 *
	 * @param name name of the {@link FunctionDefinition}.
	 * @return an optional function definition
	 */
	default Optional<FunctionDefinition> getFunctionDefinition(String name) {
		return Optional.empty();
	}

	// user defined types, operators, rules, etc
}
