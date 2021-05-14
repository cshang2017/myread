package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;

/**
 * An {@link Operation} that describes the catalog/database switch statements,
 * e.g. USE CATALOG or USE [catalogName.]dataBaseName.
 *
 * <p>Different sub operations can represent their special meanings. For example, a
 * use catalog operation means switching current catalog to another,
 * while use database operation means switching current database.
 */
@Internal
public interface UseOperation extends Operation {
}
