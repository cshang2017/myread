package org.apache.flink.table.planner.expressions;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.resolver.ExpressionResolver;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.tools.RelBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapContext;

/**
 * Planner expression resolver for {@link UnresolvedCallExpression}.
 */
public class CallExpressionResolver {

	private final ExpressionResolver resolver;

	public CallExpressionResolver(RelBuilder relBuilder) {
		FlinkContext context = unwrapContext(relBuilder.getCluster());
		this.resolver = ExpressionResolver.resolverFor(
				context.getTableConfig(),
				name -> Optional.empty(),
				context.getFunctionCatalog().asLookup(str -> {
					throw new TableException("We should not need to lookup any expressions at this point");
				}),
				context.getCatalogManager().getDataTypeFactory())
			.build();
	}

	public ResolvedExpression resolve(Expression expression) {
		List<ResolvedExpression> resolved = resolver.resolve(Collections.singletonList(expression));
		Preconditions.checkArgument(resolved.size() == 1);
		return resolved.get(0);
	}
}
