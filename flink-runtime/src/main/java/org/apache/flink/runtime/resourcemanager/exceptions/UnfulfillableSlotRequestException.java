
package org.apache.flink.runtime.resourcemanager.exceptions;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;


/**
 * Exception denoting that a slot request can not be fulfilled by any slot in the cluster.
 * This usually indicates that the slot request should not be pended or retried.
 */
public class UnfulfillableSlotRequestException extends ResourceManagerException {

	public UnfulfillableSlotRequestException(AllocationID allocationId, ResourceProfile resourceProfile) {
		super("Could not fulfill slot request " + allocationId + ". "
			+ "Requested resource profile (" + resourceProfile + ") is unfulfillable.");
	}
}
