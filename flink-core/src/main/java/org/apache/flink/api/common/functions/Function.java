
package org.apache.flink.api.common.functions;

import org.apache.flink.annotation.Public;

/**
 * The base interface for all user-defined functions.
 *
 * <p>This interface is empty in order to allow extending interfaces to
 * be SAM (single abstract method) interfaces that can be implemented via Java 8 lambdas.</p>
 */
@Public
public interface Function extends java.io.Serializable {
}
