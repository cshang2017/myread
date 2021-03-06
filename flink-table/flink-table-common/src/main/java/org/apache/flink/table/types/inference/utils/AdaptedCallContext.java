

package org.apache.flink.table.types.inference.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsAvoidingCast;

/**
 * Helper context that deals with adapted arguments.
 *
 * <p>For example, if an argument needs to be casted to a target type, an expression that was a
 * literal before is not a literal anymore in this call context.
 */
@Internal
public final class AdaptedCallContext implements CallContext {

	private final CallContext originalContext;

	private final @Nullable DataType outputDataType;

	private List<DataType> expectedArguments;

	public AdaptedCallContext(CallContext originalContext, @Nullable DataType outputDataType) {
		this.originalContext = originalContext;
		this.expectedArguments = originalContext.getArgumentDataTypes();
		this.outputDataType = outputDataType;
	}

	public void setExpectedArguments(List<DataType> expectedArguments) {
		Preconditions.checkArgument(this.expectedArguments.size() == expectedArguments.size());
		this.expectedArguments = expectedArguments;
	}

	@Override
	public DataTypeFactory getDataTypeFactory() {
		return originalContext.getDataTypeFactory();
	}

	@Override
	public FunctionDefinition getFunctionDefinition() {
		return originalContext.getFunctionDefinition();
	}

	@Override
	public boolean isArgumentLiteral(int pos) {
		if (isCasted(pos)) {
			return false;
		}
		return originalContext.isArgumentLiteral(pos);
	}

	@Override
	public boolean isArgumentNull(int pos) {
		// null remains null regardless of casting
		return originalContext.isArgumentNull(pos);
	}

	@Override
	public <T> Optional<T> getArgumentValue(int pos, Class<T> clazz) {
		if (isCasted(pos)) {
			return Optional.empty();
		}
		return originalContext.getArgumentValue(pos, clazz);
	}

	@Override
	public String getName() {
		return originalContext.getName();
	}

	@Override
	public List<DataType> getArgumentDataTypes() {
		return expectedArguments;
	}

	@Override
	public Optional<DataType> getOutputDataType() {
		return Optional.ofNullable(outputDataType);
	}

	private boolean isCasted(int pos) {
		final LogicalType originalType = originalContext.getArgumentDataTypes().get(pos).getLogicalType();
		final LogicalType expectedType = expectedArguments.get(pos).getLogicalType();
		return !supportsAvoidingCast(originalType, expectedType);
	}
}
