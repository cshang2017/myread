package org.apache.flink.table.factories;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;

/**
 * Factory for creating a {@link DecodingFormat} for {@link DeserializationSchema}.
 *
 * @see FactoryUtil#createTableFactoryHelper(DynamicTableFactory, DynamicTableFactory.Context)
 */
@PublicEvolving
public interface DeserializationFormatFactory extends DecodingFormatFactory<DeserializationSchema<RowData>> {
  // interface is used for discovery but is already fully specified by the generics
}
