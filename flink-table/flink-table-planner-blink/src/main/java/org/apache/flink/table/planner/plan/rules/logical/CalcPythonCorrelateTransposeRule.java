package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalRel;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.rules.physical.stream.StreamExecCorrelateRule;
import org.apache.flink.table.planner.plan.utils.PythonUtil;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexUtil;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Rule will transpose the conditions after the Python correlate node if the join type is inner join.
 */
public class CalcPythonCorrelateTransposeRule extends RelOptRule {

	public static final CalcPythonCorrelateTransposeRule INSTANCE =
		new CalcPythonCorrelateTransposeRule();

	private CalcPythonCorrelateTransposeRule() {
		super(operand(FlinkLogicalCorrelate.class,
			operand(FlinkLogicalRel.class, any()),
			operand(FlinkLogicalCalc.class, any())),
			"CalcPythonCorrelateTransposeRule");
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		FlinkLogicalCorrelate correlate = call.rel(0);
		FlinkLogicalCalc right = call.rel(2);
		JoinRelType joinType = correlate.getJoinType();
		FlinkLogicalCalc mergedCalc = StreamExecCorrelateRule.getMergedCalc(right);
		FlinkLogicalTableFunctionScan scan = StreamExecCorrelateRule.getTableScan(mergedCalc);
		return joinType == JoinRelType.INNER &&
			PythonUtil.isPythonCall(scan.getCall(), null) &&
			mergedCalc.getProgram().getCondition() != null;
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		FlinkLogicalCorrelate correlate = call.rel(0);
		FlinkLogicalCalc right = call.rel(2);
		RexBuilder rexBuilder = call.builder().getRexBuilder();
		FlinkLogicalCalc mergedCalc = StreamExecCorrelateRule.getMergedCalc(right);
		FlinkLogicalTableFunctionScan tableScan = StreamExecCorrelateRule.getTableScan(mergedCalc);
		RexProgram mergedCalcProgram = mergedCalc.getProgram();

		InputRefRewriter inputRefRewriter = new InputRefRewriter(
			correlate.getRowType().getFieldCount() - mergedCalc.getRowType().getFieldCount());
		List<RexNode> correlateFilters = RelOptUtil
			.conjunctions(mergedCalcProgram.expandLocalRef(mergedCalcProgram.getCondition()))
			.stream()
			.map(x -> x.accept(inputRefRewriter))
			.collect(Collectors.toList());

		FlinkLogicalCorrelate newCorrelate = new FlinkLogicalCorrelate(
			correlate.getCluster(),
			correlate.getTraitSet(),
			correlate.getLeft(),
			tableScan,
			correlate.getCorrelationId(),
			correlate.getRequiredColumns(),
			correlate.getJoinType());

		RexNode topCalcCondition = RexUtil.composeConjunction(rexBuilder, correlateFilters);
		RexProgram rexProgram = new RexProgramBuilder(
			newCorrelate.getRowType(), rexBuilder).getProgram();
		FlinkLogicalCalc newTopCalc = new FlinkLogicalCalc(
			newCorrelate.getCluster(),
			newCorrelate.getTraitSet(),
			newCorrelate,
			RexProgram.create(
				newCorrelate.getRowType(),
				rexProgram.getExprList(),
				topCalcCondition,
				newCorrelate.getRowType(),
				rexBuilder));

		call.transformTo(newTopCalc);
	}
}
