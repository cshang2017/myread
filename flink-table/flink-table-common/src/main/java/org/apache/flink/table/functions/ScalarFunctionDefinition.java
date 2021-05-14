package org.apache.flink.table.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.util.Preconditions;

import java.util.Objects;
import java.util.Set;

/**
 * A "marker" function definition of a user-defined scalar function that uses the old type system
 * stack.
 *
 * <p>This class can be dropped once we introduce a new type inference.
 */
@Internal
public final class ScalarFunctionDefinition implements FunctionDefinition {

	private final String name;
	private final ScalarFunction scalarFunction;

	public ScalarFunctionDefinition(String name, ScalarFunction scalarFunction) {
		this.name = Preconditions.checkNotNull(name);
		this.scalarFunction = Preconditions.checkNotNull(scalarFunction);
	}

	public String getName() {
		return name;
	}

	public ScalarFunction getScalarFunction() {
		return scalarFunction;
	}

	@Override
	public FunctionKind getKind() {
		return FunctionKind.SCALAR;
	}

	@Override
	public TypeInference getTypeInference(DataTypeFactory factory) {
		throw new TableException("Functions implemented for the old type system are not supported.");
	}

	@Override
	public Set<FunctionRequirement> getRequirements() {
		return scalarFunction.getRequirements();
	}

	@Override
	public boolean isDeterministic() {
		return scalarFunction.isDeterministic();
	}

	@Override
	public String toString() {
		return name;
	}
}
