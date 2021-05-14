

package org.apache.flink.table.sources;

import org.apache.flink.table.expressions.Expression;

import java.util.List;

/**
 * Adds support for filtering push-down to a {@link TableSource}.
 * A {@link TableSource} extending this interface is able to filter records before returning.
 */
public interface FilterableTableSource<T> {

	/**
	 * Check and pick all predicates this table source can support. The passed in predicates
	 * have been translated in conjunctive form, and table source can only pick those predicates
	 * that it supports.
	 *
	 * <p><strong>WARNING:</strong> Flink planner will push down PlannerExpressions
	 * (which are defined in flink-table-planner module), while Blink planner will push down {@link Expression}s.
	 * So the implementation for Flink planner and Blink planner should be different and incompatible.
	 * PlannerExpression will be removed in the future.
	 *
	 * <p>After trying to push predicates down, we should return a new {@link TableSource}
	 * instance which holds all pushed down predicates. Even if we actually pushed nothing down,
	 * it is recommended that we still return a new {@link TableSource} instance since we will
	 * mark the returned instance as filter push down has been tried.
	 *
	 * <p>We also should note to not changing the form of the predicates passed in. It has been
	 * organized in CNF conjunctive form, and we should only take or leave each element from the
	 * list. Don't try to reorganize the predicates if you are absolutely confident with that.
	 *
	 * @param predicates A list contains conjunctive predicates, you should pick and remove all
	 *                   expressions that can be pushed down. The remaining elements of this list
	 *                   will further evaluated by framework.
	 * @return A new cloned instance of {@link TableSource} with or without any filters been
	 *         pushed into it.
	 */
	TableSource<T> applyPredicate(List<Expression> predicates);

	/**
	 * Return the flag to indicate whether filter push down has been tried.
	 * Must return true on the returned instance of {@link #applyPredicate}.
	 */
	boolean isFilterPushedDown();
}
