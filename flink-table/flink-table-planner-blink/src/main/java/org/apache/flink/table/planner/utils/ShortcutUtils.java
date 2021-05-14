package org.apache.flink.table.planner.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.tools.RelBuilder;

/**
 * Utilities for quick access of commonly used instances (like {@link FlinkTypeFactory}) without
 * long chains of getters or casting like {@code (FlinkTypeFactory) agg.getCluster.getTypeFactory()}.
 */
@Internal
public final class ShortcutUtils {

	public static FlinkTypeFactory unwrapTypeFactory(SqlOperatorBinding operatorBinding) {
		return unwrapTypeFactory(operatorBinding.getTypeFactory());
	}

	public static FlinkTypeFactory unwrapTypeFactory(RelNode relNode) {
		return unwrapTypeFactory(relNode.getCluster());
	}

	public static FlinkTypeFactory unwrapTypeFactory(RelOptCluster cluster) {
		return unwrapTypeFactory(cluster.getTypeFactory());
	}

	public static FlinkTypeFactory unwrapTypeFactory(RelDataTypeFactory typeFactory) {
		return (FlinkTypeFactory) typeFactory;
	}

	public static FlinkContext unwrapContext(RelBuilder relBuilder) {
		return unwrapContext(relBuilder.getCluster());
	}

	public static FlinkContext unwrapContext(RelNode relNode) {
		return unwrapContext(relNode.getCluster());
	}

	public static FlinkContext unwrapContext(RelOptCluster cluster) {
		return unwrapContext(cluster.getPlanner());
	}

	public static FlinkContext unwrapContext(RelOptPlanner planner) {
		return unwrapContext(planner.getContext());
	}

	public static FlinkContext unwrapContext(Context context) {
		return context.unwrap(FlinkContext.class);
	}

	private ShortcutUtils() {
		// no instantiation
	}
}
