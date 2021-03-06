

package org.apache.flink.table.api.constraints;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Integrity constraints, generally referred to simply as constraints, define the valid
 * states of SQL-data by constraining the values in the base tables.
 */
@PublicEvolving
public interface Constraint {
	String getName();

	/**
	 * Constraints can either be enforced or non-enforced. If a constraint is enforced it will
	 * be checked whenever any SQL statement is executed that results in data or schema changes.
	 * If the constraint is not enforced the owner of the data is responsible for ensuring data
	 * integrity. Flink will rely the information is valid and might use it for query
	 * optimisations.
	 */
	boolean isEnforced();

	/**
	 * Tells what kind of constraint it is, e.g. PRIMARY KEY, UNIQUE, ...
	 */
	ConstraintType getType();

	/**
	 * Prints the constraint in a readable way.
	 */
	String asSummaryString();

	/**
	 * Type of the constraint.
	 *
	 * <p>Unique constraints:
	 * <ul>
	 *     <li>UNIQUE - is satisfied if and only if there do not exist two rows that have same
	 *     non-null values in the unique columns </li>
	 *     <li>PRIMARY KEY - additionally to UNIQUE constraint, it requires none of the values in
	 *     specified columns be a null value. Moreover there can be only a single PRIMARY KEY
	 *     defined for a Table.</li>
	 * </ul>
	 */
	enum ConstraintType {
		PRIMARY_KEY,
		UNIQUE_KEY
	}
}
