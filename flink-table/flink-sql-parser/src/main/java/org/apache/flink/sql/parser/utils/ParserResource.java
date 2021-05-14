package org.apache.flink.sql.parser.utils;

import org.apache.flink.sql.parser.impl.ParseException;

import org.apache.calcite.runtime.Resources;

/**
 * Compiler-checked resources for the Flink SQL parser.
 */
public interface ParserResource {

	/** Resources. */
	ParserResource RESOURCE = Resources.create(ParserResource.class);

	@Resources.BaseMessage("Multiple WATERMARK statements is not supported yet.")
	Resources.ExInst<ParseException> multipleWatermarksUnsupported();

	@Resources.BaseMessage("OVERWRITE expression is only used with INSERT statement.")
	Resources.ExInst<ParseException> overwriteIsOnlyUsedWithInsert();
}
