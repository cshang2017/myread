package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.DataType;

import java.util.List;

/**
 * Expression that has been fully resolved and validated.
 *
 * <p>Compared to {@link Expression}, resolved expressions do not contain unresolved subexpressions
 * anymore and provide an output data type for the computation result.
 *
 * <p>Instances of this class describe a fully parameterized, immutable expression that can be serialized
 * and persisted.
 *
 * <p>Resolved expression are the output of the API to the planner and are pushed from the planner
 * into interfaces, for example, for predicate push-down.
 */
@PublicEvolving
public interface ResolvedExpression extends Expression {

	/**
	 * Returns a string that fully serializes this instance. The serialized string can be used for storing
	 * the query in, for example, a {@link org.apache.flink.table.catalog.Catalog} as a view.
	 *
	 * @return detailed string for persisting in a catalog
	 */
	default String asSerializableString() {
		throw new UnsupportedOperationException("Expressions are not string serializable for now.");
	}

	/**
	 * Returns the data type of the computation result.
	 */
	DataType getOutputDataType();

	List<ResolvedExpression> getResolvedChildren();
}
