

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * This rule is copied from Calcite's {@link org.apache.calcite.rel.rules.ProjectJoinTransposeRule}.
 * Modification:
 * - Does not match SEMI/ANTI join now
 */

/**
 * Planner rule that pushes a {@link org.apache.calcite.rel.core.Project}
 * past a {@link org.apache.calcite.rel.core.Join}
 * by splitting the projection into a projection on top of each child of
 * the join.
 */
public class FlinkProjectJoinTransposeRule extends RelOptRule {
	public static final FlinkProjectJoinTransposeRule INSTANCE =
			new FlinkProjectJoinTransposeRule(expr -> true,
					RelFactories.LOGICAL_BUILDER);

	//~ Instance fields --------------------------------------------------------

	/**
	 * Condition for expressions that should be preserved in the projection.
	 */
	private final PushProjector.ExprCondition preserveExprCondition;

	//~ Constructors -----------------------------------------------------------

	/**
	 * Creates a ProjectJoinTransposeRule with an explicit condition.
	 *
	 * @param preserveExprCondition Condition for expressions that should be
	 *                              preserved in the projection
	 */
	public FlinkProjectJoinTransposeRule(
			PushProjector.ExprCondition preserveExprCondition,
			RelBuilderFactory relFactory) {
		super(
				operand(Project.class,
						operand(Join.class, any())),
				relFactory, null);
		this.preserveExprCondition = preserveExprCondition;
	}

	//~ Methods ----------------------------------------------------------------

	// implement RelOptRule
	public void onMatch(RelOptRuleCall call) {
		Project origProj = call.rel(0);
		final Join join = call.rel(1);

		if (!join.getJoinType().projectsRight()) {
			return; // TODO: support SEMI/ANTI join later
		}
		// locate all fields referenced in the projection and join condition;
		// determine which inputs are referenced in the projection and
		// join condition; if all fields are being referenced and there are no
		// special expressions, no point in proceeding any further
		PushProjector pushProject =
				new PushProjector(
						origProj,
						join.getCondition(),
						join,
						preserveExprCondition,
						call.builder());
		if (pushProject.locateAllRefs()) {
			return;
		}

		// create left and right projections, projecting only those
		// fields referenced on each side
		RelNode leftProjRel =
				pushProject.createProjectRefsAndExprs(
						join.getLeft(),
						true,
						false);
		RelNode rightProjRel =
				pushProject.createProjectRefsAndExprs(
						join.getRight(),
						true,
						true);

		// convert the join condition to reference the projected columns
		RexNode newJoinFilter = null;
		int[] adjustments = pushProject.getAdjustments();
		if (join.getCondition() != null) {
			List<RelDataTypeField> projJoinFieldList = new ArrayList<>();
			projJoinFieldList.addAll(
					join.getSystemFieldList());
			projJoinFieldList.addAll(
					leftProjRel.getRowType().getFieldList());
			projJoinFieldList.addAll(
					rightProjRel.getRowType().getFieldList());
			newJoinFilter =
					pushProject.convertRefsAndExprs(
							join.getCondition(),
							projJoinFieldList,
							adjustments);
		}

		// create a new join with the projected children
		Join newJoinRel =
				join.copy(
						join.getTraitSet(),
						newJoinFilter,
						leftProjRel,
						rightProjRel,
						join.getJoinType(),
						join.isSemiJoinDone());

		// put the original project on top of the join, converting it to
		// reference the modified projection list
		RelNode topProject =
				pushProject.createNewProject(newJoinRel, adjustments);

		call.transformTo(topProject);
	}
}

// End FlinkProjectJoinTransposeRule.java
