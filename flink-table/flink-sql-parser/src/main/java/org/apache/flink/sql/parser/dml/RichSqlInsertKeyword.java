package org.apache.flink.sql.parser.dml;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Defines the keywords that can occur immediately after the "INSERT" keyword.
 *
 * <p>Standard SQL has no such keywords, but extension projects may define them.
 *
 * <p>This class is almost an extension of {@link org.apache.calcite.sql.SqlInsertKeyword}.
 */
public enum RichSqlInsertKeyword {
	OVERWRITE;

	/**
	 * Creates a parse-tree node representing an occurrence of this keyword
	 * at a particular position in the parsed text.
	 */
	public SqlLiteral symbol(SqlParserPos pos) {
		return SqlLiteral.createSymbol(this, pos);
	}
}
