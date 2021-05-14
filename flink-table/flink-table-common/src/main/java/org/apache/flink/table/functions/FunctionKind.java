package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Categorizes the semantics of a {@link FunctionDefinition}.
 */
@PublicEvolving
public enum FunctionKind {

	SCALAR,

	TABLE,

	ASYNC_TABLE,

	AGGREGATE,

	TABLE_AGGREGATE,

	OTHER
}
