

package org.apache.flink.table.expressions.resolver.lookups;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.expressions.TableReferenceExpression;

import java.util.Optional;

/**
 * Provides a way to look up table reference by the name of the table.
 */
@Internal
public interface TableReferenceLookup {

	/**
	 * Tries to resolve given name to {@link TableReferenceExpression}.
	 *
	 * @param name name of table to look for
	 * @return resolved field reference or empty if could not find table with given name.
	 */
	Optional<TableReferenceExpression> lookupTable(String name);
}
