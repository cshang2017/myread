package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.rest.handler.async.AbstractAsynchronousOperationHandlers;
import org.apache.flink.runtime.rest.handler.async.OperationKey;
import org.apache.flink.runtime.rest.messages.TriggerId;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * A pair of {@link JobID} and {@link TriggerId} used as a key to a hash based
 * collection.
 *
 * @see AbstractAsynchronousOperationHandlers
 */
@Immutable
public class AsynchronousJobOperationKey extends OperationKey {

	private final JobID jobId;

	private AsynchronousJobOperationKey(final TriggerId triggerId, final JobID jobId) {
		super(triggerId);
		this.jobId = requireNonNull(jobId);
	}

	public static AsynchronousJobOperationKey of(final TriggerId triggerId, final JobID jobId) {
		return new AsynchronousJobOperationKey(triggerId, jobId);
	}

}
