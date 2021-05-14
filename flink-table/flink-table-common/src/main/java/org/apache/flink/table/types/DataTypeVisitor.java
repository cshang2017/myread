package org.apache.flink.table.types;

import org.apache.flink.annotation.PublicEvolving;

/**
 * The visitor definition of {@link DataType}. The visitor transforms a data type into
 * instances of {@code R}.
 */
@PublicEvolving
public interface DataTypeVisitor<R> {

	R visit(AtomicDataType atomicDataType);

	R visit(CollectionDataType collectionDataType);

	R visit(FieldsDataType fieldsDataType);

	R visit(KeyValueDataType keyValueDataType);
}
