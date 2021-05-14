package org.apache.flink.table.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.util.Preconditions;

import java.util.Objects;
import java.util.Set;

/**
 * A "marker" function definition of an user-defined table function that uses the old type system
 * stack.
 *
 * <p>This class can be dropped once we introduce a new type inference.
 */
@Internal
public final class TableFunctionDefinition implements FunctionDefinition {

	private final String name;
	private final TableFunction<?> tableFunction;
	private final TypeInformation<?> resultType;

	public TableFunctionDefinition(
			String name,
			TableFunction<?> tableFunction,
			TypeInformation<?> resultType) {
		this.name = Preconditions.checkNotNull(name);
		this.tableFunction = Preconditions.checkNotNull(tableFunction);
		this.resultType = Preconditions.checkNotNull(resultType);
	}

	public String getName() {
		return name;
	}

	public TableFunction<?> getTableFunction() {
		return tableFunction;
	}

	public TypeInformation<?> getResultType() {
		return resultType;
	}

	@Override
	public FunctionKind getKind() {
		return FunctionKind.TABLE;
	}

	@Override
	public TypeInference getTypeInference(DataTypeFactory typeFactory) {
		throw new TableException("Functions implemented for the old type system are not supported.");
	}

	@Override
	public Set<FunctionRequirement> getRequirements() {
		return tableFunction.getRequirements();
	}

	@Override
	public boolean isDeterministic() {
		return tableFunction.isDeterministic();
	}


	@Override
	public String toString() {
		return name;
	}
}
