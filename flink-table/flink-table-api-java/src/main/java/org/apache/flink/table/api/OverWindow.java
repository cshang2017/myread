package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.expressions.Expression;

import java.util.List;
import java.util.Optional;

/**
 * An over window specification.
 *
 * <p>Similar to SQL, over window aggregates compute an aggregate for each input row over a range
 * of its neighboring rows.
 */
@PublicEvolving
public final class OverWindow {

	private final Expression alias;
	private final List<Expression> partitioning;
	private final Expression order;
	private final Expression preceding;
	private final Optional<Expression> following;

	OverWindow(
		Expression alias,
		List<Expression> partitionBy,
		Expression orderBy,
		Expression preceding,
		Optional<Expression> following) {
		this.alias = alias;
		this.partitioning = partitionBy;
		this.order = orderBy;
		this.preceding = preceding;
		this.following = following;
	}

	public Expression getAlias() {
		return alias;
	}

	public List<Expression> getPartitioning() {
		return partitioning;
	}

	public Expression getOrder() {
		return order;
	}

	public Expression getPreceding() {
		return preceding;
	}

	public Optional<Expression> getFollowing() {
		return following;
	}
}
