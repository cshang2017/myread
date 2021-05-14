package org.apache.flink.table.sources.wmstrategies;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.descriptors.Rowtime;

import java.util.HashMap;
import java.util.Map;

/**
 * A strategy which indicates the watermarks should be preserved from the underlying datastream.
 */
@PublicEvolving
public final class PreserveWatermarks extends WatermarkStrategy {

	public static final PreserveWatermarks INSTANCE = new PreserveWatermarks();

	@Override
	public boolean equals(Object obj) {
		return obj instanceof PreserveWatermarks;
	}

	@Override
	public int hashCode() {
		return PreserveWatermarks.class.hashCode();
	}

	@Override
	public Map<String, String> toProperties() {
		Map<String, String> map = new HashMap<>();
		map.put(Rowtime.ROWTIME_WATERMARKS_TYPE, Rowtime.ROWTIME_WATERMARKS_TYPE_VALUE_FROM_SOURCE);
		return map;
	}
}
