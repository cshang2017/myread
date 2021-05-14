package org.apache.flink.configuration;

import org.apache.flink.annotation.Internal;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Pipeline options that are not meant to be used by the user.
 */
@Internal
public class PipelineOptionsInternal {

	public static final ConfigOption<String> PIPELINE_FIXED_JOB_ID =
			key("$internal.pipeline.job-id")
					.stringType()
					.noDefaultValue()
					.withDescription("**DO NOT USE** The static JobId to be used for the specific pipeline. " +
							"For fault-tolerance, this value needs to stay the same across runs.");
}
