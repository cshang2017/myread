package org.apache.flink.runtime.broadcast;

/**
 * Indicates that the {@link BroadcastVariableMaterialization} has materialized the broadcast variable at some point
 * but discarded it already.
 */
public class MaterializationExpiredException extends Exception {
}
