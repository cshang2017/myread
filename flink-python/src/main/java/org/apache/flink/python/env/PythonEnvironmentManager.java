
package org.apache.flink.python.env;

import org.apache.flink.annotation.Internal;
import org.apache.beam.model.pipeline.v1.RunnerApi;

/**
 * The base interface of python environment manager which is used to create the Environment object and the
 * RetrievalToken of Beam Fn API.
 */
@Internal
public interface PythonEnvironmentManager extends AutoCloseable {

	/**
	 * Initialize the environment manager.
	 */
	void open() ;

	/**
	 * Creates the Environment object used in Apache Beam Fn API.
	 *
	 * @return The Environment object which represents the environment(process, docker, etc) the python worker would run
	 *         in.
	 */
	RunnerApi.Environment createEnvironment() ;

	/**
	 * Creates the RetrievalToken used in Apache Beam Fn API. It contains a list of files which need to transmit through
	 * ArtifactService provided by Apache Beam.
	 *
	 * @return The path of the RetrievalToken file.
	 */
	String createRetrievalToken() ;

	/**
	 * Returns the boot log of the Python Environment.
	 */
	String getBootLog();
}
