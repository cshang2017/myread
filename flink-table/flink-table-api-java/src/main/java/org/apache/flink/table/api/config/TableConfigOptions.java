package org.apache.flink.table.api.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.SqlDialect;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds {@link org.apache.flink.configuration.ConfigOption}s used by
 * table planner.
 *
 * <p>NOTE: All option keys in this class must start with "table".
 */
@PublicEvolving
public class TableConfigOptions {
	private TableConfigOptions() {}

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<Boolean> TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED =
			key("table.dynamic-table-options.enabled")
					.booleanType()
					.defaultValue(false)
					.withDescription("Enable or disable the OPTIONS hint used to specify table options" +
							"dynamically, if disabled, an exception would be thrown " +
							"if any OPTIONS hint is specified");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<String> TABLE_SQL_DIALECT = key("table.sql-dialect")
			.stringType()
			.defaultValue(SqlDialect.DEFAULT.name().toLowerCase())
			.withDescription("The SQL dialect defines how to parse a SQL query. " +
					"A different SQL dialect may support different SQL grammar. " +
					"Currently supported dialects are: default and hive");
}
