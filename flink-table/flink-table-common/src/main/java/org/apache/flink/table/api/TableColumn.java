

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/**
 * A table column represents a table column's structure with
 * column name, column data type and computation expression(if it is a computed column).
 */
@PublicEvolving
public class TableColumn {

	private final String name;
	private final DataType type;
	@Nullable
	private final String expr;

	//~ Constructors -----------------------------------------------------------

	/**
	 * Creates a {@link TableColumn} instance.
	 *
	 * @param name Column name
	 * @param type Column data type
	 * @param expr Column computation expression if it is a computed column
	 */
	private TableColumn(
			String name,
			DataType type,
			@Nullable String expr) {
		this.name = name;
		this.type = type;
		this.expr = expr;
	}

	//~ Methods ----------------------------------------------------------------

	/**
	 * Creates a table column from given name and data type.
	 */
	public static TableColumn of(String name, DataType type) {
		return new TableColumn(name, type, null);
	}

	/**
	 * Creates a table column from given name and computation expression.
	 *
	 * @param name Name of the column
	 * @param expression SQL-style expression
	 */
	public static TableColumn of(String name, DataType type, String expression) {
		return new TableColumn(name, type, expression);
	}


	//~ Getter/Setter ----------------------------------------------------------

	/** Returns data type of this column. */
	public DataType getType() {
		return this.type;
	}

	/** Returns name of this column. */
	public String getName() {
		return name;
	}

	/** Returns computation expression of this column. Or empty if this column
	 * is not a computed column. */
	public Optional<String> getExpr() {
		return Optional.ofNullable(this.expr);
	}

	/**
	 * Returns if this column is a computed column that is generated from an expression.
	 *
	 * @return true if this column is generated
	 */
	public boolean isGenerated() {
		return this.expr != null;
	}

}
