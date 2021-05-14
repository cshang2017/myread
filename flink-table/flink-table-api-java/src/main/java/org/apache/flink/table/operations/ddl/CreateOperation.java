

package org.apache.flink.table.operations.ddl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.operations.Operation;

/**
 * A {@link Operation} that describes the DDL statements, e.g. CREATE TABLE or CREATE FUNCTION.
 *
 * <p>Different sub operations can have their special instances. For example, a
 * create table operation will have a {@link org.apache.flink.table.catalog.CatalogTable} instance,
 * while a create function operation will have a
 * {@link org.apache.flink.table.catalog.CatalogFunction} instance.
 */
@Internal
public interface CreateOperation extends Operation {
}
