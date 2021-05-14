package org.apache.flink.api.common;

import org.apache.flink.annotation.PublicEvolving;

/**
 * This constraint indicates when a task should be scheduled considering its inputs status.
 */
@PublicEvolving
public enum InputDependencyConstraint {

	/**
	 * Schedule the task if any input is consumable.
	 */
	ANY,

	/**
	 * Schedule the task if all the inputs are consumable.
	 */
	ALL
}
