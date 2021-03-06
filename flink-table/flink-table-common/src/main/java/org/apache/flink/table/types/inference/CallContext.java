

package org.apache.flink.table.types.inference;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.List;
import java.util.Optional;

/**
 * Provides details about a function call during {@link TypeInference}.
 */
@PublicEvolving
public interface CallContext {

	/**
	 * Enables to lookup types in a catalog and resolve RAW types.
	 */
	DataTypeFactory getDataTypeFactory();

	/**
	 * Returns the function definition that defines the function currently being called.
	 */
	FunctionDefinition getFunctionDefinition();

	/**
	 * Returns whether the argument at the given position is a value literal.
	 */
	boolean isArgumentLiteral(int pos);

	/**
	 * Returns {@code true} if the argument at the given position is a literal and {@code null},
	 * {@code false} otherwise.
	 *
	 * <p>Use {@link #isArgumentLiteral(int)} before to check if the argument is actually a literal.
	 */
	boolean isArgumentNull(int pos);

	/**
	 * Returns the literal value of the argument at the given position, given that the argument is a
	 * literal, is not null, and can be expressed as an instance of the provided class.
	 *
	 * <p>It supports conversions to default conversion classes of {@link LogicalType LogicalTypes}.
	 * This method should not be called with other classes.
	 *
	 * <p>Use {@link #isArgumentLiteral(int)} before to check if the argument is actually a literal.
	 */
	<T> Optional<T> getArgumentValue(int pos, Class<T> clazz);

	/**
	 * Returns the function's name usually referencing the function in a catalog.
	 *
	 * <p>Note: The name is meant for debugging purposes only.
	 */
	String getName();

	/**
	 * Returns a resolved list of the call's argument types. It also includes a type for every argument
	 * in a vararg function call.
	 */
	List<DataType> getArgumentDataTypes();

	/**
	 * Returns the inferred output data type of the function call.
	 *
	 * <p>It does this by inferring the input argument data type using
	 * {@link ArgumentTypeStrategy#inferArgumentType(CallContext, int, boolean)} of a wrapping call (if
	 * available) where this function call is an argument. For example, {@code takes_string(this_function(NULL))}
	 * would lead to a {@link DataTypes#STRING()} because the wrapping call expects a string argument.
	 */
	Optional<DataType> getOutputDataType();

	/**
	 * Creates a validation error for exiting the type inference process with a meaningful exception.
	 */
	default ValidationException newValidationError(String message, Object... args) {
		return new ValidationException(String.format(message, args));
	}
}
