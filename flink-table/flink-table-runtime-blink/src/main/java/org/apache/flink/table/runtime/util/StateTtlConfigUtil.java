package org.apache.flink.table.runtime.util;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;

/**
 * Utility to create a {@link StateTtlConfig} object.
 * */
public class StateTtlConfigUtil {

	/**
	 * Creates a {@link StateTtlConfig} depends on retentionTime parameter.
	 * @param retentionTime State ttl time which unit is MILLISECONDS.
	 */
	public static StateTtlConfig createTtlConfig(long retentionTime) {
		if (retentionTime > 0) {
			return StateTtlConfig
				.newBuilder(Time.milliseconds(retentionTime))
				.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
				.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
				.build();
		} else {
			return StateTtlConfig.DISABLED;
		}
	}
}
