package org.apache.flink.runtime.jobmanager.scheduler;

/**
 * Defines the location preference constraint.
 *
 * <p> Currently, we support that all input locations have to be taken into consideration
 * and only those which are known at scheduling time. Note that if all input locations
 * are considered, then the scheduling operation can potentially take a while until all
 * inputs have locations assigned.
 */
public enum LocationPreferenceConstraint {
	ALL, // wait for all inputs to have a location assigned
	ANY // only consider those inputs who already have a location assigned
}
