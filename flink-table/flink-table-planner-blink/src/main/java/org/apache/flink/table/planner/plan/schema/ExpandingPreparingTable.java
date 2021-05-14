package org.apache.flink.table.planner.plan.schema;

import org.apache.flink.table.planner.plan.stats.FlinkStatistic;

import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;

import javax.annotation.Nullable;

/**
 * A common subclass for all tables that are expanded to sub-query (views). It handles
 * recursive expanding of sub-views automatically.
 */
public abstract class ExpandingPreparingTable extends FlinkPreparingTableBase {
	protected ExpandingPreparingTable(
		@Nullable RelOptSchema relOptSchema,
		RelDataType rowType,
		Iterable<String> names,
		FlinkStatistic statistic) {
		super(relOptSchema, rowType, names, statistic);
	}

	/**
	 * Converts the table to a {@link RelNode}. Does not need to expand
	 * any nested scans of an {@link ExpandingPreparingTable}. Those will be expanded recursively.
	 *
	 * @return a relational tree
	 */
	protected abstract RelNode convertToRel(ToRelContext context);

	@Override
	public final RelNode toRel(RelOptTable.ToRelContext context) {
		return expand(context);
	}

	private RelNode expand(RelOptTable.ToRelContext context) {
		final RelNode rel = convertToRel(context);
		// Expand any views
		return rel.accept(
			new RelShuttleImpl() {
				@Override
				public RelNode visit(TableScan scan) {
					final RelOptTable table = scan.getTable();
					if (table instanceof ExpandingPreparingTable) {
						return ((ExpandingPreparingTable) table).expand(context);
					}
					return super.visit(scan);
				}
			});
	}
}
