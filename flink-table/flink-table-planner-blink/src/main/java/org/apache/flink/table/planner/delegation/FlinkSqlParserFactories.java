package org.apache.flink.table.planner.delegation;

import org.apache.flink.sql.parser.hive.impl.FlinkHiveSqlParserImpl;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;

import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.validate.SqlConformance;

/**
 * A util method to create SqlParserImplFactory according to SqlConformance.
 */
public class FlinkSqlParserFactories {

	private FlinkSqlParserFactories() {
	}

	public static SqlParserImplFactory create(SqlConformance conformance) {
		if (conformance == FlinkSqlConformance.HIVE) {
			return FlinkHiveSqlParserImpl.FACTORY;
		} else {
			return FlinkSqlParserImpl.FACTORY;
		}
	}
}
