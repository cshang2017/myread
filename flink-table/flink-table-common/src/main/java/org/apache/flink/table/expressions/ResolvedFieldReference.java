package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.sources.FieldComputer;
import org.apache.flink.util.Preconditions;

/**
 * A reference to a field in an input which has been resolved.
 *
 * <p>Note: This interface is added as a temporary solution. It is used to keep api compatible
 * for {@link FieldComputer}. In the long term, this interface can be removed.
 */
@PublicEvolving
public class ResolvedFieldReference {

	private final String name;
	private final TypeInformation<?> resultType;
	private final int fieldIndex;

	public ResolvedFieldReference(String name, TypeInformation<?> resultType, int fieldIndex) {
		Preconditions.checkArgument(fieldIndex >= 0, "Index of field should be a positive number");
		this.name = Preconditions.checkNotNull(name, "Field name must not be null.");
		this.resultType = Preconditions.checkNotNull(resultType, "Field result type must not be null.");
		this.fieldIndex = fieldIndex;
	}

	public TypeInformation<?> resultType() {
		return resultType;
	}

	public String name() {
		return name;
	}

	public int fieldIndex() {
		return fieldIndex;
	}
}
