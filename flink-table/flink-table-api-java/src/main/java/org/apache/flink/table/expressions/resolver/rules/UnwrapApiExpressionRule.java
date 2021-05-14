
package org.apache.flink.table.expressions.resolver.rules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Unwraps all {@link ApiExpression}.
 */
@Internal
final class UnwrapApiExpressionRule implements ResolverRule {
	@Override
	public List<Expression> apply(
			List<Expression> expression,
			ResolutionContext context) {
		return expression.stream().map(ApiExpressionUtils::unwrapFromApi).collect(Collectors.toList());
	}
}
