package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.delegation.Planner;

import java.util.List;

/**
 * A {@link Operation} that describes the DML queries such as e.g. INSERT or conversion to a
 * DataStream.
 *
 * <p>A tree of {@link QueryOperation} with a {@link ModifyOperation} on top
 * represents a runnable query that can be transformed into a graph of
 * {@link Transformation}
 * via {@link Planner#translate(List)}
 *
 * @see QueryOperation
 */
@Internal
public interface ModifyOperation extends Operation {
	QueryOperation getChild();

	<T> T accept(ModifyOperationVisitor<T> visitor);
}
