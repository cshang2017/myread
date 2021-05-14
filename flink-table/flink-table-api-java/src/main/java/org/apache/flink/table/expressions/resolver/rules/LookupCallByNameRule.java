

package org.apache.flink.table.expressions.resolver.rules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.resolver.LookupCallResolver;
import org.apache.flink.table.functions.FunctionDefinition;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Resolves {@link org.apache.flink.table.expressions.LookupCallExpression} to
 * a corresponding {@link FunctionDefinition}.
 */
@Internal
final class LookupCallByNameRule implements ResolverRule {
	@Override
	public List<Expression> apply(List<Expression> expression, ResolutionContext context) {
		LookupCallResolver lookupCallResolver = new LookupCallResolver(context.functionLookup());
		return expression.stream().map(expr -> expr.accept(lookupCallResolver)).collect(Collectors.toList());
	}
}
