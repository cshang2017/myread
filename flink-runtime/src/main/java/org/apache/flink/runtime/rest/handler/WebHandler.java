package org.apache.flink.runtime.rest.handler;

/**
 * Marker interface for web handlers which can describe their paths.
 */
public interface WebHandler {

	/**
	 * Returns an array of REST URL's under which this handler can be registered.
	 *
	 * @return array containing REST URL's under which this handler can be registered.
	 */
	String[] getPaths();
}
