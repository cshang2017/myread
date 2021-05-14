
package org.apache.flink.table.data.util;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.HashMap;
import java.util.Map;

/**
 * Utilities for {@link MapData}.
 */
public final class MapDataUtil {

	/**
	 * Converts a {@link MapData} into Java {@link Map}, the keys and values of the Java map
	 * still holds objects of internal data structures.
	 */
	public static Map<Object, Object> convertToJavaMap(
			MapData map, LogicalType keyType, LogicalType valueType) {
		ArrayData keyArray = map.keyArray();
		ArrayData valueArray = map.valueArray();
		Map<Object, Object> javaMap = new HashMap<>();
		for (int i = 0; i < map.size(); i++) {
			Object key = ArrayData.get(keyArray, i, keyType);
			Object value = ArrayData.get(valueArray, i, valueType);
			javaMap.put(key, value);
		}
		return javaMap;
	}
}
