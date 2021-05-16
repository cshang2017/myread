

package org.apache.flink.runtime.query;

/**
 * Thrown if there is no {@link KvStateLocation} found for the requested
 * registration name.
 *
 * <p>This indicates that the requested KvState instance is not registered
 * under this name (yet).
 */
public class UnknownKvStateLocation extends Exception {

	public UnknownKvStateLocation(String registrationName) {
		super("No KvStateLocation found for KvState instance with name '" + registrationName + "'.");
	}
}
