package org.apache.flink.python;

import org.apache.flink.annotation.Internal;

/**
 * The base interface of runner which is responsible for the execution of Python functions.
 *
 * @param <IN> Type of the input elements.
 */
@Internal
public interface PythonFunctionRunner<IN> {

	/**
	 * Prepares the Python function runner, such as preparing the Python execution environment, etc.
	 */
	void open() throws Exception;

	/**
	 * Tear-down the Python function runner.
	 */
	void close() throws Exception;

	/**
	 * Prepares to process the next bundle of elements.
	 */
	void startBundle() throws Exception;

	/**
	 * Forces to finish the processing of the current bundle of elements. It will flush
	 * the data cached in the data buffer for processing and retrieves the state mutations (if exists)
	 * made by the Python function. The call blocks until all of the outputs produced by this
	 * bundle have been received.
	 */
	void finishBundle() throws Exception;

	/**
	 * Executes the Python function with the input element. It's not required to execute
	 * the Python function immediately. The runner may choose to buffer the input element and
	 * execute them in batch for better performance.
	 */
	void processElement(IN element) throws Exception;
}
