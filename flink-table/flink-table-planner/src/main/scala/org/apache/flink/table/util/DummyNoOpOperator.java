package org.apache.flink.table.util;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.NoOpOperator;

import java.io.IOException;

/**
 * This is dummy {@link NoOpOperator}, which context is {@link DummyExecutionEnvironment}.
 */
public class DummyNoOpOperator<IN> extends NoOpOperator<IN> {

	public DummyNoOpOperator(
			ExecutionEnvironment dummyExecEnv,
			DataSet<IN> input,
			TypeInformation<IN> resultType) {
		super(dummyExecEnv.createInput(new DummyInputFormat(), resultType), resultType);

		setInput(input);
	}

	/**
	 * Dummy file input format implementation.
	 */
	public static class DummyInputFormat<IN> extends FileInputFormat<IN> {

		@Override
		public boolean reachedEnd() throws IOException {
			return false;
		}

		@Override
		public IN nextRecord(IN reuse) throws IOException {
			return null;
		}
	}
}
