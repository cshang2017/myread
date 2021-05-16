package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;

/**
 * Base class for exceptions thrown by the {@link SlotManager}.
 */
public class SlotManagerException extends ResourceManagerException {
	public SlotManagerException(String message) {
		super(message);
	}

	public SlotManagerException(String message, Throwable cause) {
		super(message, cause);
	}

	public SlotManagerException(Throwable cause) {
		super(cause);
	}
}
