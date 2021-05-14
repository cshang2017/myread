package org.apache.flink.table.planner.catalog;

import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.planner.plan.schema.ExpandingPreparingTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;

import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import javax.annotation.Nullable;

import java.util.List;

/**
 * A bridge between a Flink's specific {@link CatalogView} and a Calcite's
 * {@link org.apache.calcite.plan.RelOptTable}. It implements parsing and conversion from sql string to
 * {@link org.apache.calcite.rel.RelNode}.
 */
public class SqlCatalogViewTable extends ExpandingPreparingTable {
	private final CatalogView view;
	private final List<String> viewPath;

	public SqlCatalogViewTable(
			@Nullable RelOptSchema relOptSchema,
			RelDataType rowType,
			Iterable<String> names,
			FlinkStatistic statistic,
			CatalogView view,
			List<String> viewPath) {
		super(relOptSchema, rowType, names, statistic);
		this.view = view;
		this.viewPath = viewPath;
	}

	@Override
	public RelNode convertToRel(ToRelContext context) {
		RelNode original = context
				.expandView(rowType, view.getExpandedQuery(), viewPath, names)
				.project();
		return RelOptUtil.createCastRel(original, rowType, true);
	}
}
