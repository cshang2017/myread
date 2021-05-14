

package org.apache.flink.table.expressions.resolver;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.FunctionLookup;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.LookupCallExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.utils.ApiExpressionDefaultVisitor;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedCall;

/**
 * Resolves calls with function names to calls with actual function definitions.
 */
@Internal
public class LookupCallResolver extends ApiExpressionDefaultVisitor<Expression> {

	private final FunctionLookup functionLookup;

	public LookupCallResolver(FunctionLookup functionLookup) {
		this.functionLookup = functionLookup;
	}

	public Expression visit(LookupCallExpression lookupCall) {
		final FunctionLookup.Result result = functionLookup.lookupFunction(lookupCall.getUnresolvedName())
			.orElseThrow(() -> new ValidationException("Undefined function: " + lookupCall.getUnresolvedName()));

		return unresolvedCall(
			result.getFunctionIdentifier(),
			result.getFunctionDefinition(),
			resolveChildren(lookupCall.getChildren()));
	}

	public Expression visit(UnresolvedCallExpression unresolvedCall) {
		return unresolvedCall.replaceArgs(resolveChildren(unresolvedCall.getChildren()));
	}

	private List<Expression> resolveChildren(List<Expression> lookupChildren) {
		return lookupChildren
			.stream()
			.map(child -> child.accept(this))
			.collect(Collectors.toList());
	}

	@Override
	public Expression visitNonApiExpression(Expression other) {
		// LookupCallResolver might be called outside of ExpressionResolver, thus we need to additionally
		// handle the ApiExpressions here
		if (other instanceof ApiExpression) {
			return ((ApiExpression) other).toExpr().accept(this);
		} else {
			return defaultMethod(other);
		}
	}

	@Override
	protected Expression defaultMethod(Expression expression) {
		return expression;
	}
}
