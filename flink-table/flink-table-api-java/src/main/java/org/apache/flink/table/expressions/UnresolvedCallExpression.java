package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Unresolved call expression for calling a function identified by a {@link FunctionDefinition}.
 *
 * <p>This is a purely API facing expression with unvalidated arguments and unknown output data type.
 *
 * <p>A unresolved call contains:
 * <ul>
 *     <li>a {@link FunctionDefinition} that identifies the function to be called</li>
 *     <li>an optional {@link FunctionIdentifier} that tracks the origin of a function</li>
 * </ul>
 */
@PublicEvolving
public final class UnresolvedCallExpression implements Expression {

	private final @Nullable FunctionIdentifier functionIdentifier;

	private final FunctionDefinition functionDefinition;

	private final List<Expression> args;

	UnresolvedCallExpression(
			FunctionIdentifier functionIdentifier,
			FunctionDefinition functionDefinition,
			List<Expression> args) {
		this.functionIdentifier =
			Preconditions.checkNotNull(functionIdentifier, "Function identifier must not be null.");
		this.functionDefinition =
			Preconditions.checkNotNull(functionDefinition, "Function definition must not be null.");
		this.args = Collections.unmodifiableList(Preconditions.checkNotNull(args, "Arguments must not be null."));
	}

	UnresolvedCallExpression(
			FunctionDefinition functionDefinition,
			List<Expression> args) {
		this.functionIdentifier = null;
		this.functionDefinition =
			Preconditions.checkNotNull(functionDefinition, "Function definition must not be null.");
		this.args = Collections.unmodifiableList(Preconditions.checkNotNull(args, "Arguments must not be null."));
	}

	public Optional<FunctionIdentifier> getFunctionIdentifier() {
		return Optional.ofNullable(functionIdentifier);
	}

	public FunctionDefinition getFunctionDefinition() {
		return functionDefinition;
	}

	public UnresolvedCallExpression replaceArgs(List<Expression> args) {
		if (functionIdentifier == null) {
			return new UnresolvedCallExpression(
				functionDefinition,
				args);
		}
		return new UnresolvedCallExpression(
			functionIdentifier,
			functionDefinition,
			args);
	}

	public CallExpression resolve(List<ResolvedExpression> args, DataType dataType) {
		if (functionIdentifier == null) {
			return new CallExpression(
				functionDefinition,
				args,
				dataType);
		}
		return new CallExpression(
			functionIdentifier,
			functionDefinition,
			args,
			dataType);
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
		return this.args;
	}

	@Override
	public <R> R accept(ExpressionVisitor<R> visitor) {
		return visitor.visit(this);
	}



	@Override
	public String toString() {
		return asSummaryString();
	}
}
