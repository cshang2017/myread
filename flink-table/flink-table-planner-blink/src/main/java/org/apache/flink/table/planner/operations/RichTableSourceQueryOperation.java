package org.apache.flink.table.planner.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;
import org.apache.flink.table.operations.TableSourceQueryOperation;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@link TableSourceQueryOperation} with {@link FlinkStatistic} and qualifiedName.
 * TODO this class should be deleted after unique key in TableSchema is ready
 * and setting catalog statistic to TableSourceTable in DatabaseCalciteSchema is ready
 *
 * <p>This is only used for testing.
 */
@Internal
public class RichTableSourceQueryOperation<T> extends TableSourceQueryOperation<T> {
	private final FlinkStatistic statistic;
	private final ObjectIdentifier identifier;

	public RichTableSourceQueryOperation(
			ObjectIdentifier identifier,
			TableSource<T> tableSource,
			FlinkStatistic statistic) {
		super(tableSource, false);
		Preconditions.checkArgument(tableSource instanceof StreamTableSource,
				"Blink planner should always use StreamTableSource.");
		this.statistic = statistic;
		this.identifier = identifier;
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> args = new HashMap<>();
		args.put("fields", getTableSource().getTableSchema().getFieldNames());
		if (statistic != FlinkStatistic.UNKNOWN()) {
			args.put("statistic", statistic.toString());
		}

		return OperationUtils.formatWithChildren("TableSource", args, getChildren(), Operation::asSummaryString);
	}

	public ObjectIdentifier getIdentifier() {
		return identifier;
	}

	public FlinkStatistic getStatistic() {
		return statistic;
	}
}
