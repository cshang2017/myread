package org.apache.flink.table.planner.expressions;

import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;

import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Dummy wrapper for expressions that were converted to RexNode in a different way.
 */
public class RexNodeExpression implements ResolvedExpression {

	private RexNode rexNode;
	private DataType outputDataType;

	public RexNodeExpression(RexNode rexNode, DataType outputDataType) {
		this.rexNode = rexNode;
		this.outputDataType = outputDataType;
	}

	public RexNode getRexNode() {
		return rexNode;
	}

	@Override
	public String asSummaryString() {
		return rexNode.toString();
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
	public DataType getOutputDataType() {
		return outputDataType;
	}

	@Override
	public List<ResolvedExpression> getResolvedChildren() {
		return new ArrayList<>();
	}
}
