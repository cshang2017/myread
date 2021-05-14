

package org.apache.flink.api.java.operators;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.java.DataSet;

/**
 *
 * @param <IN> The type of the data set consumed by this operator.
 * @param <OUT> The type of the data set produced by this operator.
 */
@Public
public interface CustomUnaryOperation<IN, OUT> {

	void setInput(DataSet<IN> inputData);

	DataSet<OUT> createResult();
}
