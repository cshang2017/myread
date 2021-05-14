

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.sinks.TableSink;

import java.util.Collections;

/**
 * DML operation that tells to write to the given sink.
 */
@Internal
public class UnregisteredSinkModifyOperation<T> implements ModifyOperation {

	private final TableSink<T> sink;
	private final QueryOperation child;

	public UnregisteredSinkModifyOperation(TableSink<T> sink, QueryOperation child) {
		this.sink = sink;
		this.child = child;
	}

	public TableSink<T> getSink() {
		return sink;
	}

	@Override
	public QueryOperation getChild() {
		return child;
	}

	@Override
	public String asSummaryString() {
		return OperationUtils.formatWithChildren(
			"UnregisteredSink",
			Collections.emptyMap(),
			Collections.singletonList(child),
			Operation::asSummaryString);
	}

	@Override
	public <R> R accept(ModifyOperationVisitor<R> visitor) {
		return visitor.visit(this);
	}
}
