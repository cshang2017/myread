package org.apache.flink.runtime.source.event;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * An event to request splits, sent typically from the Source Reader to the Source Enumerator.
 *
 * <p>This event optionally carries the hostname of the location where the reader runs, to support
 * locality-aware work assignment.
 */
public final class RequestSplitEvent implements OperatorEvent {

	@Nullable
	private final String hostName;

	/**
	 * Creates a new {@code RequestSplitEvent} with no host information.
	 */
	public RequestSplitEvent() {
		this(null);
	}

	/**
	 * Creates a new {@code RequestSplitEvent} with a hostname.
	 */
	public RequestSplitEvent(@Nullable String hostName) {
		this.hostName = hostName;
	}

	// ------------------------------------------------------------------------

	@Nullable
	public String hostName() {
		return hostName;
	}


}
