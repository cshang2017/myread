

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;

/**
 * An {@link Operation} that show one kind of objects,
 * e.g. USE CATALOGS, USE DATABASES, SHOW TABLES, SHOW FUNCTIONS.
 */
@Internal
public interface ShowOperation extends Operation {
}
