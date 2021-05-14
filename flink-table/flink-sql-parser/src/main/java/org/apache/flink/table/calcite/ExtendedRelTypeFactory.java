package org.apache.flink.table.calcite;

import org.apache.flink.annotation.Internal;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

/**
 * A factory for creating {@link RelDataType} instances including Flink-specific extensions.
 *
 * <p>This interface exists because the parser module has no access to the planner's type factory.
 */
@Internal
public interface ExtendedRelTypeFactory extends RelDataTypeFactory {

	/**
	 * Creates a RAW type such as {@code RAW('org.my.Class', 'sW3Djsds...')}.
	 */
	RelDataType createRawType(String className, String serializerString);
}
