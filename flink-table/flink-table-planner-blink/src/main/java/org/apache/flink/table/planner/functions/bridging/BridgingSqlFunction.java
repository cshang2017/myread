package org.apache.flink.table.planner.functions.bridging;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createName;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createParamTypes;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlFunctionCategory;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlIdentifier;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlOperandTypeChecker;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlOperandTypeInference;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlReturnTypeInference;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Bridges {@link FunctionDefinition} to Calcite's representation of a scalar or table function
 * (either a system or user-defined function).
 */
@Internal
public final class BridgingSqlFunction extends SqlFunction {

	private final DataTypeFactory dataTypeFactory;

	private final FlinkTypeFactory typeFactory;

	private final @Nullable FunctionIdentifier identifier;

	private final FunctionDefinition definition;

	private final TypeInference typeInference;

	private BridgingSqlFunction(
			DataTypeFactory dataTypeFactory,
			FlinkTypeFactory typeFactory,
			SqlKind kind,
			@Nullable FunctionIdentifier identifier,
			FunctionDefinition definition,
			TypeInference typeInference) {
		super(
			createName(identifier, definition),
			createSqlIdentifier(identifier),
			kind,
			createSqlReturnTypeInference(dataTypeFactory, definition, typeInference),
			createSqlOperandTypeInference(dataTypeFactory, definition, typeInference),
			createSqlOperandTypeChecker(dataTypeFactory, definition, typeInference),
			createParamTypes(typeFactory, typeInference),
			createSqlFunctionCategory(identifier));

		this.dataTypeFactory = dataTypeFactory;
		this.typeFactory = typeFactory;
		this.identifier = identifier;
		this.definition = definition;
		this.typeInference = typeInference;
	}

	/**
	 * Creates an instance of a scalar or table function (either a system or user-defined function).
	 *
	 * @param dataTypeFactory used for creating {@link DataType}
	 * @param typeFactory used for bridging to {@link RelDataType}
	 * @param kind commonly used SQL standard function; use {@link SqlKind#OTHER_FUNCTION} if this function
	 *             cannot be mapped to a common function kind.
	 * @param identifier catalog identifier
	 * @param definition system or user-defined {@link FunctionDefinition}
	 * @param typeInference type inference logic
	 */
	public static BridgingSqlFunction of(
			DataTypeFactory dataTypeFactory,
			FlinkTypeFactory typeFactory,
			SqlKind kind,
			@Nullable FunctionIdentifier identifier,
			FunctionDefinition definition,
			TypeInference typeInference) {

		checkState(
			definition.getKind() == FunctionKind.SCALAR || definition.getKind() == FunctionKind.TABLE,
			"Scalar or table function kind expected.");

		return new BridgingSqlFunction(
			dataTypeFactory,
			typeFactory,
			kind,
			identifier,
			definition,
			typeInference);
	}

	public DataTypeFactory getDataTypeFactory() {
		return dataTypeFactory;
	}

	public FlinkTypeFactory getTypeFactory() {
		return typeFactory;
	}

	public Optional<FunctionIdentifier> getIdentifier() {
		return Optional.ofNullable(identifier);
	}

	public FunctionDefinition getDefinition() {
		return definition;
	}

	public TypeInference getTypeInference() {
		return typeInference;
	}

	@Override
	public List<String> getParamNames() {
		if (typeInference.getNamedArguments().isPresent()) {
			return typeInference.getNamedArguments().get();
		}
		return super.getParamNames();
	}

	@Override
	public boolean isDeterministic() {
		return definition.isDeterministic();
	}
}
