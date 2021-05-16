

package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;

import java.io.IOException;
import java.util.Collection;

/**
 * Interface for all classes that want to participate in the archiving of job-related json responses.
 */
public interface JsonArchivist {

	/**
	 * Returns a {@link Collection} of {@link ArchivedJson}s containing JSON responses and their respective REST URL
	 * for a given job.
	 *
	 * <p>The collection should contain one entry for every response that could be generated for the given
	 * job, for example one entry for each task. The REST URLs should be unique and must not contain placeholders.
	 *
	 * @param graph AccessExecutionGraph for which the responses should be generated
	 *
	 * @return Collection containing an ArchivedJson for every response that could be generated for the given job
	 * @throws IOException thrown if the JSON generation fails
	 */
	Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException;
}
