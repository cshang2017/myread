package org.apache.flink.api.common;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Specifies to which extent user-defined functions are analyzed in order
 * to give the Flink optimizer an insight of UDF internals and inform
 * the user about common implementation mistakes.
 *
 * The analyzer gives hints about:
 *  - ForwardedFields semantic properties
 *  - Warnings if static fields are modified by a Function
 *  - Warnings if a FilterFunction modifies its input objects
 *  - Warnings if a Function returns null
 *  - Warnings if a tuple access uses a wrong index
 *  - Information about the number of object creations (for manual optimization)
 *
 * @deprecated The code analysis code has been removed and this enum has no effect.
 */
@PublicEvolving
@Deprecated
public enum CodeAnalysisMode {

	/**
	 * Code analysis does not take place.
	 */
	DISABLE,

	/**
	 * Hints for improvement of the program are printed to the log.
	 */
	HINT,

	/**
	 * The program will be automatically optimized with knowledge from code
	 * analysis.
	 */
	OPTIMIZE;

}
