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
 * A "marker" function definition of an user-defined aggregate function that uses the old type system
 * stack.
 *
 * <p>This class can be dropped once we introduce a new type inference.
 */
@Internal
public final class AggregateFunctionDefinition implements FunctionDefinition {

	private final String name;
	private final AggregateFunction<?, ?> aggregateFunction;
	private final TypeInformation<?> resultTypeInfo;
	private final TypeInformation<?> accumulatorTypeInfo;

	public AggregateFunctionDefinition(
			String name,
			AggregateFunction<?, ?> aggregateFunction,
			TypeInformation<?> resultTypeInfo,
			TypeInformation<?> accTypeInfo) {
		this.name = Preconditions.checkNotNull(name);
		this.aggregateFunction = Preconditions.checkNotNull(aggregateFunction);
		this.resultTypeInfo = Preconditions.checkNotNull(resultTypeInfo);
		this.accumulatorTypeInfo = Preconditions.checkNotNull(accTypeInfo);
	}

	public String getName() {
		return name;
	}

	public AggregateFunction<?, ?> getAggregateFunction() {
		return aggregateFunction;
	}

	public TypeInformation<?> getResultTypeInfo() {
		return resultTypeInfo;
	}

	public TypeInformation<?> getAccumulatorTypeInfo() {
		return accumulatorTypeInfo;
	}

	@Override
	public FunctionKind getKind() {
		return FunctionKind.AGGREGATE;
	}

	@Override
	public TypeInference getTypeInference(DataTypeFactory typeFactory) {
		throw new TableException("Functions implemented for the old type system are not supported.");
	}

	@Override
	public Set<FunctionRequirement> getRequirements() {
		return aggregateFunction.getRequirements();
	}

	@Override
	public boolean isDeterministic() {
		return aggregateFunction.isDeterministic();
	}


	@Override
	public String toString() {
		return name;
	}
}
