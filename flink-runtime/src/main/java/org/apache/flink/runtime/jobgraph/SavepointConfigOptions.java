package org.apache.flink.runtime.jobgraph;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * The {@link ConfigOption configuration options} used when restoring from a savepoint.
 */
@PublicEvolving
public class SavepointConfigOptions {

	/**
	 * The path to a savepoint that will be used to bootstrap the pipeline's state.
	 */
	public static final ConfigOption<String> SAVEPOINT_PATH =
			key("execution.savepoint.path")
					.stringType()
					.noDefaultValue()
					.withDescription("Path to a savepoint to restore the job from (for example hdfs:///flink/savepoint-1537).");

	/**
	 * A flag indicating if we allow Flink to skip savepoint state that cannot be restored,
	 * e.g. because the corresponding operator has been removed.
	 */
	public static final ConfigOption<Boolean> SAVEPOINT_IGNORE_UNCLAIMED_STATE =
			key("execution.savepoint.ignore-unclaimed-state")
					.booleanType()
					.defaultValue(false)
					.withDescription("Allow to skip savepoint state that cannot be restored. " +
							"Allow this if you removed an operator from your pipeline after the savepoint was triggered.");
}
