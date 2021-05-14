package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;

import java.time.Duration;

/**
 * {@link ConfigOption}s specific for a single execution of a user program.
 */
@PublicEvolving
public class ExecutionOptions {

	/**
	 * Should be moved to {@code ExecutionCheckpointingOptions} along with
	 * {@code ExecutionConfig#useSnapshotCompression}, which should be put into {@code CheckpointConfig}.
	 */
	public static final ConfigOption<Boolean> SNAPSHOT_COMPRESSION =
		ConfigOptions.key("execution.checkpointing.snapshot-compression")
			.booleanType()
			.defaultValue(false)
		.withDescription("Tells if we should use compression for the state snapshot data or not");

	public static final ConfigOption<Duration> BUFFER_TIMEOUT =
		ConfigOptions.key("execution.buffer-timeout")
			.durationType()
			.defaultValue(Duration.ofMillis(100))
			.withDescription(Description.builder()
				.text("The maximum time frequency (milliseconds) for the flushing of the output buffers. By default " +
					"the output buffers flush frequently to provide low latency and to aid smooth developer " +
					"experience. Setting the parameter can result in three logical modes:")
				.list(
					TextElement.text("A positive value triggers flushing periodically by that interval"),
					TextElement.text("0 triggers flushing after every record thus minimizing latency"),
					TextElement.text("-1 ms triggers flushing only when the output buffer is full thus maximizing " +
						"throughput")
				)
				.build());
}
