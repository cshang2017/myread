package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A call expression where the target function has not been resolved yet.
 *
 * <p>Instead of a {@link FunctionDefinition}, the call is identified by the function's name and needs to be lookup in
 * a catalog
 */
@PublicEvolving
public final class LookupCallExpression implements Expression {

	private final String unresolvedName;

	private final List<Expression> args;

	LookupCallExpression(String unresolvedFunction, List<Expression> args) {
		this.unresolvedName = Preconditions.checkNotNull(unresolvedFunction);
		this.args = Collections.unmodifiableList(Preconditions.checkNotNull(args));
	}

	public String getUnresolvedName() {
		return unresolvedName;
	}

	@Override
	public String asSummaryString() {
		final List<String> argList = args.stream().map(Object::toString).collect(Collectors.toList());
		return unresolvedName + "(" + String.join(", ", argList) + ")";
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
