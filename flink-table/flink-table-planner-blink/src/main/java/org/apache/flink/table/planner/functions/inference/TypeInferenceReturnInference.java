

package org.apache.flink.table.planner.functions.inference;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeInferenceUtil;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;
import static org.apache.flink.table.types.inference.TypeInferenceUtil.createInvalidCallException;
import static org.apache.flink.table.types.inference.TypeInferenceUtil.createUnexpectedException;
import static org.apache.flink.table.types.inference.TypeInferenceUtil.inferOutputType;

/**
 * A {@link SqlReturnTypeInference} backed by {@link TypeInference}.
 *
 * <p>Note: This class must be kept in sync with {@link TypeInferenceUtil}.
 */
@Internal
public final class TypeInferenceReturnInference implements SqlReturnTypeInference {

	private final DataTypeFactory dataTypeFactory;

	private final FunctionDefinition definition;

	private final TypeInference typeInference;

	public TypeInferenceReturnInference(
			DataTypeFactory dataTypeFactory,
			FunctionDefinition definition,
			TypeInference typeInference) {
		this.dataTypeFactory = dataTypeFactory;
		this.definition = definition;
		this.typeInference = typeInference;
	}

	@Override
	public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
		final CallContext callContext = new OperatorBindingCallContext(
			dataTypeFactory,
			definition,
			opBinding);
			return inferReturnTypeOrError(unwrapTypeFactory(opBinding), callContext);
		
	}

	// --------------------------------------------------------------------------------------------

	private RelDataType inferReturnTypeOrError(FlinkTypeFactory typeFactory, CallContext callContext) {
		final LogicalType inferredType = inferOutputType(callContext, typeInference.getOutputTypeStrategy())
			.getLogicalType();
		return typeFactory.createFieldTypeFromLogicalType(inferredType);
	}
}
