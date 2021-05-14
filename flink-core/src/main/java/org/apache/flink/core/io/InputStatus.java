package org.apache.flink.core.io;

import org.apache.flink.annotation.PublicEvolving;

/**
 * An {@code InputStatus} indicates the availability of data from an asynchronous input.
 * When asking an asynchronous input to produce data, it returns this status to indicate how to
 * proceed.
 *
 * <p>When the input returns {@link InputStatus#NOTHING_AVAILABLE} it means that no data is available
 * at this time, but more will (most likely) be available in the future. The asynchronous input
 * will typically offer to register a <i>Notifier</i> or to obtain a <i>Future</i> that will signal
 * the availability of new data.
 *
 * <p>When the input returns {@link InputStatus#MORE_AVAILABLE}, it can be immediately asked
 * again to produce more data. That readers from the asynchronous input can bypass subscribing to
 * a Notifier or a Future for efficiency.
 *
 * <p>When the input returns {@link InputStatus#END_OF_INPUT}, then no data will be available again
 * from this input. It has reached the end of its bounded data.
 */
@PublicEvolving
public enum InputStatus {

	/**
	 * Indicator that more data is available and the input can be called immediately again
	 * to produce more data.
	 */
	MORE_AVAILABLE,

	/**
	 * Indicator that no data is currently available, but more data will be available in the
	 * future again.
	 */
	NOTHING_AVAILABLE,

	/**
	 * Indicator that the input has reached the end of data.
	 */
	END_OF_INPUT
}
