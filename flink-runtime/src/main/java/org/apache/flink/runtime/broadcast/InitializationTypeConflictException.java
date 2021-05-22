package org.apache.flink.runtime.broadcast;

import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * Indicates that a broadcast variable was initialized with a {@link DefaultBroadcastVariableInitializer} as a
 * non-{@link java.util.List} type, and later accessed using {@link RuntimeContext#getBroadcastVariable(String)} which
 * may only return lists.
 */
public class InitializationTypeConflictException extends Exception {

	private final Class<?> type;

	public InitializationTypeConflictException(Class<?> type) {
		this.type = type;
	}

	public Class<?> getType() {
		return type;
	}
}
