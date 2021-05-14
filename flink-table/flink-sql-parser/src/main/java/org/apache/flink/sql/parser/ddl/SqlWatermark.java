package org.apache.flink.sql.parser.ddl;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

import static java.util.Objects.requireNonNull;


/**
 * Watermark statement in CREATE TABLE DDL, e.g. {@code WATERMARK FOR ts AS ts - INTERVAL '5' SECOND}.
 */
public class SqlWatermark extends SqlCall {

	private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("WATERMARK", SqlKind.OTHER);

	private final SqlIdentifier eventTimeColumnName;
	private final SqlNode watermarkStrategy;

	public SqlWatermark(SqlParserPos pos, SqlIdentifier eventTimeColumnName, SqlNode watermarkStrategy) {
		super(pos);
		this.eventTimeColumnName = requireNonNull(eventTimeColumnName);
		this.watermarkStrategy = requireNonNull(watermarkStrategy);
	}

	@Override
	public SqlOperator getOperator() {
		return OPERATOR;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(eventTimeColumnName, watermarkStrategy);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("WATERMARK");
		writer.keyword("FOR");
		eventTimeColumnName.unparse(writer, leftPrec, rightPrec);
		writer.keyword("AS");
		watermarkStrategy.unparse(writer, leftPrec, rightPrec);
	}

	public SqlIdentifier getEventTimeColumnName() {
		return eventTimeColumnName;
	}

	public SqlNode getWatermarkStrategy() {
		return watermarkStrategy;
	}
}
