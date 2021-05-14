

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeStrategy;

import java.util.Optional;

/**
 * Placeholder for a missing type strategy.
 */
@Internal
public final class MissingTypeStrategy implements TypeStrategy {

	@Override
	public Optional<DataType> inferType(CallContext callContext) {
		return Optional.empty();
	}

	@Override
	public boolean equals(Object o) {
		return this == o || o instanceof MissingTypeStrategy;
	}

	@Override
	public int hashCode() {
		return MissingTypeStrategy.class.hashCode();
	}
}
