
package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.table.data.binary.BinaryRowData;

import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Helper for null aware join.
 */
public class NullAwareJoinHelper {

	public static int[] getNullFilterKeys(boolean[] filterNulls) {
		checkNotNull(filterNulls);
		List<Integer> nullFilterKeyList = new ArrayList<>();
		for (int i = 0; i < filterNulls.length; i++) {
			if (filterNulls[i]) {
				nullFilterKeyList.add(i);
			}
		}
		return ArrayUtils.toPrimitive(nullFilterKeyList.toArray(new Integer[0]));
	}

	public static boolean shouldFilter(
			boolean nullSafe, boolean filterAllNulls,
			int[] nullFilterKeys, BinaryRowData key) {
		return !nullSafe && (filterAllNulls ? key.anyNull() : key.anyNull(nullFilterKeys));
	}
}
