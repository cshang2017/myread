

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecPythonCorrelate;
import org.apache.flink.table.planner.plan.utils.PythonUtil;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rex.RexNode;

import scala.Option;
import scala.Some;

/**
 * The physical rule is responsible for convert {@link FlinkLogicalCorrelate} to
 * {@link StreamExecPythonCorrelate}.
 */
public class StreamExecPythonCorrelateRule extends ConverterRule {

	public static final RelOptRule INSTANCE = new StreamExecPythonCorrelateRule();

	private StreamExecPythonCorrelateRule() {
		super(FlinkLogicalCorrelate.class, FlinkConventions.LOGICAL(), FlinkConventions.STREAM_PHYSICAL(),
			"StreamExecPythonCorrelateRule");
	}

	// find only calc and table function
	private boolean findTableFunction(FlinkLogicalCalc calc) {
		RelNode child = ((RelSubset) calc.getInput()).getOriginal();
		if (child instanceof FlinkLogicalTableFunctionScan) {
			FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) child;
			return PythonUtil.isPythonCall(scan.getCall(), null);
		} else if (child instanceof FlinkLogicalCalc) {
			FlinkLogicalCalc childCalc = (FlinkLogicalCalc) child;
			return findTableFunction(childCalc);
		}
		return false;
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		FlinkLogicalCorrelate correlate = call.rel(0);
		RelNode right = ((RelSubset) correlate.getRight()).getOriginal();
		if (right instanceof FlinkLogicalTableFunctionScan) {
			// right node is a table function
			FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) right;
			// return true if the table function is python table function
			return PythonUtil.isPythonCall(scan.getCall(), null);
		} else if (right instanceof FlinkLogicalCalc) {
			// a filter is pushed above the table function
			return findTableFunction((FlinkLogicalCalc) right);
		}
		return false;
	}

	@Override
	public RelNode convert(RelNode relNode) {
		StreamExecPythonCorrelateFactory factory = new StreamExecPythonCorrelateFactory(relNode);
		return factory.convertToCorrelate();
	}

	/**
	 * The factory is responsible for creating {@link StreamExecPythonCorrelate}.
	 */
	private static class StreamExecPythonCorrelateFactory {
		private final FlinkLogicalCorrelate correlate;
		private final RelTraitSet traitSet;
		private final RelNode convInput;
		private final RelNode right;

		StreamExecPythonCorrelateFactory(RelNode rel) {
			this.correlate = (FlinkLogicalCorrelate) rel;
			this.traitSet = rel.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL());
			this.convInput = RelOptRule.convert(
				correlate.getInput(0), FlinkConventions.STREAM_PHYSICAL());
			this.right = correlate.getInput(1);
		}

		StreamExecPythonCorrelate convertToCorrelate() {
			return convertToCorrelate(right, Option.empty());
		}

		private StreamExecPythonCorrelate convertToCorrelate(
			RelNode relNode,
			Option<RexNode> condition) {
			if (relNode instanceof RelSubset) {
				RelSubset rel = (RelSubset) relNode;
				return convertToCorrelate(rel.getRelList().get(0), condition);
			} else if (relNode instanceof FlinkLogicalCalc) {
				FlinkLogicalCalc calc = (FlinkLogicalCalc) relNode;
				RelNode tableScan = StreamExecCorrelateRule.getTableScan(calc);
				FlinkLogicalCalc newCalc = StreamExecCorrelateRule.getMergedCalc(calc);
				return convertToCorrelate(
					tableScan,
					Some.apply(newCalc.getProgram().expandLocalRef(newCalc.getProgram().getCondition())));
			} else {
				FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) relNode;
				return new StreamExecPythonCorrelate(
					correlate.getCluster(),
					traitSet,
					convInput,
					null,
					scan,
					condition,
					correlate.getRowType(),
					correlate.getJoinType());
			}
		}
	}
}
