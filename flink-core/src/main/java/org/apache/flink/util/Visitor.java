package org.apache.flink.util;

import org.apache.flink.annotation.Internal;

/**
 * A visitor encapsulates functionality that is applied to each node in the process of a traversal of a tree or DAG.
 */
@Internal
public interface Visitor<T extends Visitable<T>> {

	/**
	 * Method that is invoked on the visit before visiting and child nodes or descendant nodes.
	 *
	 * @return True, if the traversal should continue, false otherwise.
	 */
	boolean preVisit(T visitable);

	/**
	 * Method that is invoked after all child nodes or descendant nodes were visited.
	 */
	void postVisit(T visitable);
}
