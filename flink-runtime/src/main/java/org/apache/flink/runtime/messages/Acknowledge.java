package org.apache.flink.runtime.messages;

import java.io.Serializable;

/**
 * A generic acknowledgement message.
 */
public class Acknowledge implements Serializable {

	/** The singleton instance */
	private static final Acknowledge INSTANCE = new Acknowledge();

	/**
	 * Gets the singleton instance.
	 * @return The singleton instance.
	 */
	public static Acknowledge get() {
		return INSTANCE;
	}
	
	// ------------------------------------------------------------------------
	
	/** Private constructor to prevent instantiation */
	private Acknowledge() {}


	/**
	 * Read resolve to preserve the singleton object property.
	 * (per best practices, this should have visibility 'protected')
	 */
	protected Object readResolve() throws java.io.ObjectStreamException {
		return INSTANCE;
	}
}
