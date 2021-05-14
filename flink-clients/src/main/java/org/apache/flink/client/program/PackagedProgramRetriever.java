package org.apache.flink.client.program;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.FlinkException;

/**
 * Interface which allows to retrieve the {@link PackagedProgram}.
 */
@Internal
public interface PackagedProgramRetriever {

	/**
	 * Retrieve the {@link PackagedProgram}.
	 *
	 * @return the retrieved {@link PackagedProgram}.
	 * @throws FlinkException if the {@link PackagedProgram} could not be retrieved
	 */
	PackagedProgram getPackagedProgram() throws FlinkException;
}
