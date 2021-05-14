package org.apache.flink.table.planner.plan.nodes.exec;

/**
 * Visitor pattern for traversing a dag of {@link ExecNode} objects.
 */
public interface ExecNodeVisitor {

	/**
	 * Visits a node during a traversal.
	 *
	 * @param node ExecNode to visit
	 */
	void visit(ExecNode<?, ?> node);

}
