package org.apache.flink.runtime.executiongraph;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.Preconditions;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;

/**
 * Container class for operator/task specific information which are stored at the
 * {@link ExecutionJobVertex}. This information is shared by all sub tasks of this operator.
 */
@Getter
@AllArgsConstructor
public class TaskInformation implements Serializable {

	/** Job vertex id of the associated job vertex. */
	private final JobVertexID jobVertexId;

	private final String taskName;
	private final int numberOfSubtasks;

	/** The maximum parallelism == number of key groups. */
	private final int maxNumberOfSubtasks;

	/** Class name of the invokable to run. */
	private final String invokableClassName;

	private final Configuration taskConfiguration;

}
