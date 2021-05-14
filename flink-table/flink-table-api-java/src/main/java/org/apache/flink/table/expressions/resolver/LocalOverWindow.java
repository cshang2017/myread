package org.apache.flink.table.expressions.resolver;

import org.apache.flink.table.expressions.Expression;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/**
 * Local over window created during expression resolution.
 */
public final class LocalOverWindow {

	private Expression alias;

	private List<Expression> partitionBy;

	private Expression orderBy;

	private Expression preceding;

	private @Nullable Expression following;

	LocalOverWindow(
			Expression alias,
			List<Expression> partitionBy,
			Expression orderBy,
			Expression preceding,
			@Nullable Expression following) {
		this.alias = alias;
		this.partitionBy = partitionBy;
		this.orderBy = orderBy;
		this.preceding = preceding;
		this.following = following;
	}

	public Expression getAlias() {
		return alias;
	}

	public List<Expression> getPartitionBy() {
		return partitionBy;
	}

	public Expression getOrderBy() {
		return orderBy;
	}

	public Expression getPreceding() {
		return preceding;
	}

	public Optional<Expression> getFollowing() {
		return Optional.ofNullable(following);
	}
}
