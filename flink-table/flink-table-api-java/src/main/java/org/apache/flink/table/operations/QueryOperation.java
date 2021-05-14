
package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;

import java.util.List;

/**
 * Base class for representing an operation structure behind a user-facing {@link Table} API.
 *
 * <p>It represents an operation that can be a node of a relational query. It has a schema, that
 * can be used to validate a {@link QueryOperation} applied on top of this one.
 */
@Internal
public interface QueryOperation extends Operation {

	/**
	 * Resolved schema of this operation.
	 */
	TableSchema getTableSchema();

	/**
	 * Returns a string that fully serializes this instance. The serialized string can be used for storing
	 * the query in e.g. a {@link org.apache.flink.table.catalog.Catalog} as a view.
	 *
	 * @return detailed string for persisting in a catalog
	 * @see Operation#asSummaryString()
	 */
	default String asSerializableString() {
		throw new UnsupportedOperationException("QueryOperations are not string serializable for now.");
	}

	List<QueryOperation> getChildren();

	default  <T> T accept(QueryOperationVisitor<T> visitor) {
		return visitor.visit(this);
	}

}
