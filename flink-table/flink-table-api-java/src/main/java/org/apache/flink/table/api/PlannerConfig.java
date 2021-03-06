package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import java.util.Optional;

/**
 * The {@link PlannerConfig} holds parameters to configure the behavior of queries.
 */
@PublicEvolving
public interface PlannerConfig {

	PlannerConfig EMPTY_CONFIG = new PlannerConfig() {};

	@SuppressWarnings("unchecked")
	default <T extends PlannerConfig> Optional<T> unwrap(Class<T> type) {
		if (type.isInstance(this)) {
			return Optional.of((T) this);
		} else {
			return Optional.empty();
		}
	}
}
