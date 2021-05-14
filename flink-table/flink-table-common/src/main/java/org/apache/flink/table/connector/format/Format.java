

package org.apache.flink.table.connector.format;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;

/**
 * Base interface for connector formats.
 *
 * <p>Depending on the kind of external system, a connector might support different encodings for
 * reading and writing rows. This interface is an intermediate representation before constructing actual
 * runtime implementation.
 *
 * <p>Formats can be distinguished along two dimensions:
 * <ul>
 *     <li>Context in which the format is applied ({@link DynamicTableSource} or {@link DynamicTableSink}).
 *     <li>Runtime implementation interface that is required (e.g. {@link DeserializationSchema} or
 *     some bulk interface).</li>
 * </ul>
 *
 * <p>A {@link DynamicTableFactory} can search for a format that it is accepted by the connector.
 *
 * @see DecodingFormat
 * @see EncodingFormat
 */
@PublicEvolving
public interface Format {

	/**
	 * Returns the set of changes that a connector (and transitively the planner) can expect during
	 * runtime.
	 */
	ChangelogMode getChangelogMode();
}
