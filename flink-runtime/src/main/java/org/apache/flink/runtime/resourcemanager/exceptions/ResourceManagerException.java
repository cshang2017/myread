package org.apache.flink.runtime.resourcemanager.exceptions;

import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.util.FlinkException;

/**
 * Base class for {@link ResourceManager} exceptions.
 */
public class ResourceManagerException extends FlinkException {

	public ResourceManagerException(String message) {
		super(message);
	}

	public ResourceManagerException(String message, Throwable cause) {
		super(message, cause);
	}

	public ResourceManagerException(Throwable cause) {
		super(cause);
	}
}
