package org.apache.flink.table.sources.wmstrategies;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.utils.EncodingUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides a strategy to generate watermarks for a rowtime attribute.
 *
 * <p>A watermark strategy is either a {@code PeriodicWatermarkAssigner} or
 * {@code PunctuatedWatermarkAssigner}.
 */
@PublicEvolving
public abstract class WatermarkStrategy implements Serializable, Descriptor {

	/**
	 * This method is a default implementation that uses java serialization and it is discouraged.
	 * All implementation should provide a more specific set of properties.
	 */
	@Override
	public Map<String, String> toProperties() {
		Map<String, String> properties = new HashMap<>();
		
		properties.put(Rowtime.ROWTIME_WATERMARKS_TYPE, Rowtime.ROWTIME_WATERMARKS_TYPE_VALUE_CUSTOM);
		properties.put(Rowtime.ROWTIME_WATERMARKS_CLASS, this.getClass().getName());
		properties.put(Rowtime.ROWTIME_WATERMARKS_SERIALIZED, EncodingUtils.encodeObjectToString(this));

		return properties;
	}
}
