

package org.apache.flink.api.java.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Resulting {@link DataSet} of bulk iterations.
 * @param <T>
 */
@Internal
public class BulkIterationResultSet<T> extends DataSet<T> {

	private final IterativeDataSet<T> iterationHead;

	private final DataSet<T> nextPartialSolution;

	private final DataSet<?> terminationCriterion;

	BulkIterationResultSet(ExecutionEnvironment context,
						TypeInformation<T> type,
						IterativeDataSet<T> iterationHead,
						DataSet<T> nextPartialSolution) {
		this(context, type, iterationHead, nextPartialSolution, null);
	}

	BulkIterationResultSet(ExecutionEnvironment context,
		TypeInformation<T> type, IterativeDataSet<T> iterationHead,
		DataSet<T> nextPartialSolution, DataSet<?> terminationCriterion) {
		super(context, type);
		this.iterationHead = iterationHead;
		this.nextPartialSolution = nextPartialSolution;
		this.terminationCriterion = terminationCriterion;
	}

	public IterativeDataSet<T> getIterationHead() {
		return iterationHead;
	}

	public DataSet<T> getNextPartialSolution() {
		return nextPartialSolution;
	}

	public DataSet<?> getTerminationCriterion() {
		return terminationCriterion;
	}
}
