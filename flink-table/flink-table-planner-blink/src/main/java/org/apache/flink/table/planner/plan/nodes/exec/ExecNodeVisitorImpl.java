

package org.apache.flink.table.planner.plan.nodes.exec;

/**
 * Implement of {@link ExecNodeVisitor}.
 */
public class ExecNodeVisitorImpl implements ExecNodeVisitor {

	public void visit(ExecNode<?, ?> node) {
		visitInputs(node);
	}

	protected void visitInputs(ExecNode<?, ?> node) {
		node.getInputNodes().forEach(n -> n.accept(this));
	}
}
