

package org.apache.flink.table.api.constraints;

import org.apache.flink.annotation.PublicEvolving;

import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A unique key constraint. It can be declared also as a PRIMARY KEY.
 *
 * @see ConstraintType
 */
@PublicEvolving
public final class UniqueConstraint extends AbstractConstraint {
	private final List<String> columns;
	private final ConstraintType type;

	/**
	 * Creates a non enforced {@link ConstraintType#PRIMARY_KEY} constraint.
	 */
	public static UniqueConstraint primaryKey(String name, List<String> columns) {
		return new UniqueConstraint(name, false, ConstraintType.PRIMARY_KEY, columns);
	}

	private UniqueConstraint(
			String name,
			boolean enforced,
			ConstraintType type,
			List<String> columns) {
		super(name, enforced);

		this.columns = checkNotNull(columns);
		this.type = checkNotNull(type);
	}

	/**
	 * List of column names for which the primary key was defined.
	 */
	public List<String> getColumns() {
		return columns;
	}

	@Override
	public ConstraintType getType() {
		return type;
	}

	/**
	 * Returns constraint's summary. All constraints summary will be formatted as
	 * <pre>
	 * CONSTRAINT [constraint-name] [constraint-type] ([constraint-definition])
	 *
	 * E.g CONSTRAINT pk PRIMARY KEY (f0, f1)
	 * </pre>
	 */
	@Override
	public final String asSummaryString() {
		final String typeString;
		switch (getType()) {
			case PRIMARY_KEY:
				typeString = "PRIMARY KEY";
				break;
			case UNIQUE_KEY:
				typeString = "UNIQUE";
				break;
			default:
				throw new IllegalStateException("Unknown key type: " + getType());
		}

		return String.format("CONSTRAINT %s %s (%s)", getName(), typeString, String.join(", ", columns));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		UniqueConstraint that = (UniqueConstraint) o;
		return Objects.equals(columns, that.columns) &&
			type == that.type;
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), columns, type);
	}
}
