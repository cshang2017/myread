package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Resolved and validated call expression for calling a function.
 *
 * <p>A call contains:
 * <ul>
 *     <li>an output type</li>
 *     <li>a {@link FunctionDefinition} that identifies the function to be called</li>
 *     <li>an optional {@link ObjectIdentifier} that tracks the origin of a function</li>
 * </ul>
 */
@PublicEvolving
public final class CallExpression implements ResolvedExpression {

	private final @Nullable FunctionIdentifier functionIdentifier;

	private final FunctionDefinition functionDefinition;

	private final List<ResolvedExpression> args;

	private final DataType dataType;

	public CallExpression(
			FunctionIdentifier functionIdentifier,
			FunctionDefinition functionDefinition,
			List<ResolvedExpression> args,
			DataType dataType) {
		this.functionIdentifier =
			Preconditions.checkNotNull(functionIdentifier, "Object identifier must not be null.");
		this.functionDefinition =
			Preconditions.checkNotNull(functionDefinition, "Function definition must not be null.");
		this.args = new ArrayList<>(Preconditions.checkNotNull(args, "Arguments must not be null."));
		this.dataType = Preconditions.checkNotNull(dataType, "Data type must not be null.");
	}

	public CallExpression(
			FunctionDefinition functionDefinition,
			List<ResolvedExpression> args,
			DataType dataType) {
		this.functionIdentifier = null;
		this.functionDefinition = Preconditions.checkNotNull(functionDefinition, "Function definition must not be null.");
		this.args = new ArrayList<>(Preconditions.checkNotNull(args, "Arguments must not be null."));
		this.dataType = Preconditions.checkNotNull(dataType, "Data type must not be null.");
	}

	public Optional<FunctionIdentifier> getFunctionIdentifier() {
		return Optional.ofNullable(functionIdentifier);
	}

	public FunctionDefinition getFunctionDefinition() {
		return functionDefinition;
	}

	@Override
	public DataType getOutputDataType() {
		return dataType;
	}

	@Override
	public List<ResolvedExpression> getResolvedChildren() {
		return args;
	}

	@Override
	public String asSummaryString() {
		final String functionName;
		if (functionIdentifier == null) {
			functionName = functionDefinition.toString();
		} else {
			functionName = functionIdentifier.asSummaryString();
		}

		final String argList = args.stream()
			.map(Expression::asSummaryString)
			.collect(Collectors.joining(", ", "(", ")"));

		return functionName + argList;
	}

	@Override
	public List<Expression> getChildren() {
		return Collections.unmodifiableList(this.args);
	}

	@Override
	public <R> R accept(ExpressionVisitor<R> visitor) {
		return visitor.visit(this);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		CallExpression that = (CallExpression) o;
		return Objects.equals(functionIdentifier, that.functionIdentifier) &&
			functionDefinition.equals(that.functionDefinition) &&
			args.equals(that.args) &&
			dataType.equals(that.dataType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(functionIdentifier, functionDefinition, args, dataType);
	}

	@Override
	public String toString() {
		return asSummaryString();
	}
}
