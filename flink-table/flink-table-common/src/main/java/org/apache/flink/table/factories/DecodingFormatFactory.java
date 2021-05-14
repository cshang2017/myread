package org.apache.flink.table.factories;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;

/**
 * Base interface for configuring a {@link DecodingFormat} for {@link ScanTableSource} and {@link LookupTableSource}.
 *
 * <p>Depending on the kind of external system, a connector might support different encodings for
 * reading and writing rows. This interface helps in making such formats pluggable.
 *
 * <p>The created {@link Format} instance is an intermediate representation that can be used to construct
 * runtime implementation in a later step.
 *
 * @see FactoryUtil#createTableFactoryHelper(DynamicTableFactory, DynamicTableFactory.Context)
 *
 * @param <I> runtime interface needed by the table source
 */
@PublicEvolving
public interface DecodingFormatFactory<I> extends Factory {

	/**
	 * Creates a format from the given context and format options.
	 *
	 * <p>The format options have been projected to top-level options (e.g. from {@code key.format.ignore-errors}
	 * to {@code format.ignore-errors}).
	 */
	DecodingFormat<I> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions);
}
