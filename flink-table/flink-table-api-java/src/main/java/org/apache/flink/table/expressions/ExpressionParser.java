

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.delegation.PlannerExpressionParser;

import java.util.List;

/**
 * Parser for expressions inside a String. This parses exactly the same expressions that
 * would be accepted by the Scala Expression DSL.
 *
 * <p>{@link ExpressionParser} use {@link PlannerExpressionParser} to parse expressions.
 */
@Internal
public final class ExpressionParser {

	public static Expression parseExpression(String exprString) {
		return PlannerExpressionParser.create().parseExpression(exprString);
	}

	public static List<Expression> parseExpressionList(String expression) {
		return PlannerExpressionParser.create().parseExpressionList(expression);
	}
}
