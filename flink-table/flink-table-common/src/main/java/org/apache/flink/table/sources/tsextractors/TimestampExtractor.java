
package org.apache.flink.table.sources.tsextractors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.sources.FieldComputer;
import org.apache.flink.table.utils.EncodingUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides an expression to extract the timestamp for a rowtime attribute.
 */
@PublicEvolving
public abstract class TimestampExtractor implements FieldComputer<Long>, Serializable, Descriptor {

	@Override
	public TypeInformation<Long> getReturnType() {
		return Types.LONG;
	}

	/**
	 * This method is a default implementation that uses java serialization and it is discouraged.
	 * All implementation should provide a more specific set of properties.
	 */
	@Override
	public Map<String, String> toProperties() {
		Map<String, String> properties = new HashMap<>();
		properties.put(Rowtime.ROWTIME_TIMESTAMPS_TYPE, Rowtime.ROWTIME_TIMESTAMPS_TYPE_VALUE_CUSTOM);
		properties.put(Rowtime.ROWTIME_TIMESTAMPS_CLASS, this.getClass().getName());
		properties.put(Rowtime.ROWTIME_TIMESTAMPS_SERIALIZED, EncodingUtils.encodeObjectToString(this));
		return properties;
	}
}
