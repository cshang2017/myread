package org.apache.flink.table.api.constraints;

import org.apache.flink.annotation.Internal;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for {@link Constraint constraints}.
 */
@Internal
abstract class AbstractConstraint implements Constraint {
	private final String name;
	private final boolean enforced;

	AbstractConstraint(String name, boolean enforced) {
		this.name = checkNotNull(name);
		this.enforced = enforced;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public boolean isEnforced() {
		return enforced;
	}

	@Override
	public String toString() {
		return asSummaryString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		AbstractConstraint that = (AbstractConstraint) o;
		return enforced == that.enforced &&
			Objects.equals(name, that.name);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, enforced);
	}
}
