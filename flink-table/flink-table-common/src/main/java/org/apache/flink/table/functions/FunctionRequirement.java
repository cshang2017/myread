ackage org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Characteristics that a {@link FunctionDefinition} requires.
 */
@PublicEvolving
public enum FunctionRequirement {

	/**
	 * Requirement that an aggregate function can only be applied in an OVER window.
	 */
	OVER_WINDOW_ONLY
}
