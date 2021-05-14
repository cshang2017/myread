

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Reference to entity local to a certain {@link QueryOperation}.
 * That entity does not come from any of the Operations input. It might be for example a group
 * window in window aggregation.
 */
@Internal
public class LocalReferenceExpression implements ResolvedExpression {

	private final String name;

	private final DataType dataType;

	LocalReferenceExpression(String name, DataType dataType) {
		this.name = Preconditions.checkNotNull(name);
		this.dataType = Preconditions.checkNotNull(dataType);
	}

	public String getName() {
		return name;
	}

	@Override
	public DataType getOutputDataType() {
		return dataType;
	}

	@Override
	public List<ResolvedExpression> getResolvedChildren() {
		return Collections.emptyList();
	}

	@Override
	public String asSummaryString() {
		return name;
	}

	@Override
	public List<Expression> getChildren() {
		return Collections.emptyList();
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
