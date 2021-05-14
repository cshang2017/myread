package org.apache.flink.api.common.io.ratelimiting;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;
/**
 * An interface to create a ratelimiter
 *
 * <p>The ratelimiter is configured via {@link #setRate(long)} and
 * created via {@link #open(RuntimeContext)}.
 * An example implementation can be found {@link GuavaFlinkConnectorRateLimiter}.
 * */

@PublicEvolving
public interface FlinkConnectorRateLimiter extends Serializable {

	/**
	 * A method that can be used to create and configure a ratelimiter
	 * based on the runtimeContext.
	 * @param runtimeContext
	 */
	void open(RuntimeContext runtimeContext);

	/**
	 * Sets the desired rate for the rate limiter.
	 * @param rate
	 */
	void setRate(long rate);

	/**
	 * Acquires permits for the rate limiter.
	 */
	void acquire(int permits);

	long getRate();

	void close();
}
