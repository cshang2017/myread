package org.apache.flink.table.factories;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;

import javax.annotation.Nullable;

import java.util.Optional;

/**
 * Utility for dealing with {@link TableFactory} using the {@link TableFactoryService}.
 */
public class TableFactoryUtil {

	/**
	 * Returns a table source matching the descriptor.
	 */
	@SuppressWarnings("unchecked")
	public static <T> TableSource<T> findAndCreateTableSource(TableSourceFactory.Context context) {
			return TableFactoryService
					.find(TableSourceFactory.class, context.getTable().toProperties())
					.createTableSource(context);
	}

	/**
	 * Creates a {@link TableSource} from a {@link CatalogTable}.
	 *
	 * <p>It considers {@link Catalog#getFactory()} if provided.
	 */
	@SuppressWarnings("unchecked")
	public static <T> TableSource<T> findAndCreateTableSource(
			Catalog catalog,
			ObjectIdentifier objectIdentifier,
			CatalogTable catalogTable,
			ReadableConfig configuration) {
		TableSourceFactory.Context context = new TableSourceFactoryContextImpl(
			objectIdentifier,
			catalogTable,
			configuration);
		Optional<TableFactory> factoryOptional = catalog.getTableFactory();
		if (factoryOptional.isPresent()) {
			TableFactory factory = factoryOptional.get();
			if (factory instanceof TableSourceFactory) {
				return ((TableSourceFactory<T>) factory).createTableSource(context);
			} else {
				throw new ValidationException("Cannot query a sink-only table. "
					+ "TableFactory provided by catalog must implement TableSourceFactory");
			}
		} else {
			return findAndCreateTableSource(context);
		}
	}

	/**
	 * Returns a table sink matching the context.
	 */
	@SuppressWarnings("unchecked")
	public static <T> TableSink<T> findAndCreateTableSink(TableSinkFactory.Context context) {
			return TableFactoryService
					.find(TableSinkFactory.class, context.getTable().toProperties())
					.createTableSink(context);
	}

	/**
	 * Creates a {@link TableSink} from a {@link CatalogTable}.
	 *
	 * <p>It considers {@link Catalog#getFactory()} if provided.
	 */
	@SuppressWarnings("unchecked")
	public static <T> TableSink<T> findAndCreateTableSink(
			@Nullable Catalog catalog,
			ObjectIdentifier objectIdentifier,
			CatalogTable catalogTable,
			ReadableConfig configuration,
			boolean isStreamingMode) {
		TableSinkFactory.Context context = new TableSinkFactoryContextImpl(
			objectIdentifier,
			catalogTable,
			configuration,
			!isStreamingMode);
		if (catalog == null) {
			return findAndCreateTableSink(context);
		} else {
			return createTableSinkForCatalogTable(catalog, context)
				.orElseGet(() -> findAndCreateTableSink(context));
		}
	}

	/**
	 * Creates a table sink for a {@link CatalogTable} using table factory associated with the catalog.
	 */
	public static Optional<TableSink> createTableSinkForCatalogTable(Catalog catalog, TableSinkFactory.Context context) {
		TableFactory tableFactory = catalog.getTableFactory().orElse(null);
		if (tableFactory instanceof TableSinkFactory) {
			return Optional.ofNullable(((TableSinkFactory) tableFactory).createTableSink(context));
		}
		return Optional.empty();
	}

}
