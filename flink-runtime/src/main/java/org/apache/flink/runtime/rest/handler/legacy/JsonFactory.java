/

package org.apache.flink.runtime.rest.handler.legacy;

/**
 * A holder for the singleton Jackson JSON factory. Since the Jackson's JSON factory object
 * is a heavyweight object that is encouraged to be shared, we use a singleton instance across
 * all request handlers.
 */
public class JsonFactory {

	/** The singleton Jackson JSON factory. */
	public static final org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory JACKSON_FACTORY =
			new  org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory();

	// --------------------------------------------------------------------------------------------

	private JsonFactory() {}
}
