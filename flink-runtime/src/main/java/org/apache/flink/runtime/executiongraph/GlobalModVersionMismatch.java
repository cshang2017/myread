
package org.apache.flink.runtime.executiongraph;

/**
 * An exception that indicates a mismatch between the expected global modification version
 * of the execution graph, and the actual modification version.
 */
@Getter
public class GlobalModVersionMismatch extends Exception {


	private final long expectedModVersion;

	private final long actualModVersion;

	public GlobalModVersionMismatch(long expectedModVersion, long actualModVersion) {
		super("expected=" + expectedModVersion + ", actual=" + actualModVersion);
		this.expectedModVersion = expectedModVersion;
		this.actualModVersion = actualModVersion;
	}

}
