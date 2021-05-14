package org.apache.flink.table.factories;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;

/**
 * Factory for creating a {@link EncodingFormat} for {@link SerializationSchema}.
 *
 * @see FactoryUtil#createTableFactoryHelper(DynamicTableFactory, DynamicTableFactory.Context)
 */
@PublicEvolving
public interface SerializationFormatFactory extends EncodingFormatFactory<SerializationSchema<RowData>> {
  // interface is used for discovery but is already fully specified by the generics
}
