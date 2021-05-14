package org.apache.flink.util;

import org.apache.flink.annotation.Internal;

/**
 * This interface marks types as visitable during a traversal. The central method <i>accept(...)</i> contains the logic
 * about how to invoke the supplied {@link Visitor} on the visitable object, and how to traverse further.
 *
 * <p>This concept makes it easy to implement for example a depth-first traversal of a tree or DAG with different types of
 * logic during the traversal. The <i>accept(...)</i> method calls the visitor and then send the visitor to its children
 * (or predecessors). Using different types of visitors, different operations can be performed during the traversal, while
 * writing the actual traversal code only once.
 *
 * @see Visitor
 */
@Internal
public interface Visitable<T extends Visitable<T>> {

	/**
	 * Contains the logic to invoke the visitor and continue the traversal.
	 * Typically invokes the pre-visit method of the visitor, then sends the visitor to the children (or predecessors)
	 * and then invokes the post-visit method.
	 *
	 * <p>A typical code example is the following:
	 * <pre>{@code
	 * public void accept(Visitor<Operator> visitor) {
	 *     boolean descend = visitor.preVisit(this);
	 *     if (descend) {
	 *         if (this.input != null) {
	 *             this.input.accept(visitor);
	 *         }
	 *         visitor.postVisit(this);
	 *     }
	 * }
	 * }</pre>
	 *
	 * @param visitor The visitor to be called with this object as the parameter.
	 *
	 * @see Visitor#preVisit(Visitable)
	 * @see Visitor#postVisit(Visitable)
	 */
	void accept(Visitor<T> visitor);
}
