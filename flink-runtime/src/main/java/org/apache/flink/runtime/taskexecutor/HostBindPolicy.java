package org.apache.flink.runtime.taskexecutor;

/**
 * A host binding address mechanism policy.
 */
enum HostBindPolicy {
	NAME,
	IP;

	@Override
	public String toString() {
		return name().toLowerCase();
	}

	public static HostBindPolicy fromString(String configValue) {
			return HostBindPolicy.valueOf(configValue.toUpperCase());
	}
}
