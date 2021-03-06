

package org.apache.flink.table.expressions.resolver.rules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.FunctionLookup;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.LocalReferenceExpression;
import org.apache.flink.table.expressions.resolver.ExpressionResolver;
import org.apache.flink.table.expressions.resolver.LocalOverWindow;
import org.apache.flink.table.expressions.resolver.lookups.FieldReferenceLookup;
import org.apache.flink.table.expressions.resolver.lookups.TableReferenceLookup;
import org.apache.flink.table.functions.FunctionDefinition;

import java.util.List;
import java.util.Optional;

/**
 * Rule that can be applied during resolution of {@link Expression}. Rules are applied to a collection of expressions
 * at once, e.g. all expressions in a projection. One must consider order in which rules are applied. Some rules
 * might e.g. require that references to fields have to be already resolved.
 */
@Internal
public interface ResolverRule {

	List<Expression> apply(List<Expression> expression, ResolutionContext context);

	/**
	 * Contextual information that can be used during application of the rule. E.g. one can access fields in inputs by
	 * name etc.
	 */
	interface ResolutionContext {

		/**
		 * Access to configuration.
		 */
		ReadableConfig configuration();

		/**
		 * Access to available {@link org.apache.flink.table.expressions.FieldReferenceExpression} in inputs.
		 */
		FieldReferenceLookup referenceLookup();

		/**
		 * Access to available {@link org.apache.flink.table.expressions.TableReferenceExpression}.
		 */
		TableReferenceLookup tableLookup();

		/**
		 * Access to available {@link FunctionDefinition}s.
		 */
		FunctionLookup functionLookup();

		/**
		 * Access to {@link DataTypeFactory}.
		 */
		DataTypeFactory typeFactory();

		/**
		 * Enables the creation of resolved expressions for transformations after the actual resolution.
		 */
		ExpressionResolver.PostResolverFactory postResolutionFactory();

		/**
		 * Access to available local references.
		 */
		Optional<LocalReferenceExpression> getLocalReference(String alias);

		/**
		 * Access to available local over windows.
		 */
		Optional<LocalOverWindow> getOverWindow(Expression alias);
	}
}
