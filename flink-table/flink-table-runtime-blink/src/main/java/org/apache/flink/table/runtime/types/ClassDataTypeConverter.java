
package org.apache.flink.table.runtime.types;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.Optional;

/**
 * Utils for get {@link DataType} from a Class. It must return a DataType.
 * Convert known types by {@link TypeConversions#fromClassToDataType}.
 * Convert unknown types by {@link LegacyTypeInformationType}.
 */
public class ClassDataTypeConverter {

	/**
	 * @param clazz The class of the type.
	 * @return The DataType object for the type described by the hint.
	 * @throws InvalidTypesException Cannot extract TypeInformation from Class alone,
	 *                               because generic parameters are missing.
	 */
	public static DataType fromClassToDataType(Class<?> clazz) {
		Optional<DataType> optional = TypeConversions.fromClassToDataType(clazz);
		return optional.orElseGet(() ->
				TypeConversions.fromLegacyInfoToDataType(TypeExtractor.createTypeInfo(clazz)));
	}
}
