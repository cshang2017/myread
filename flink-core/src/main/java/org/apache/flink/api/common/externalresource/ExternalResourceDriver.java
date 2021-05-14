

package org.apache.flink.api.common.externalresource;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Set;

/**
 * Driver which takes the responsibility to manage and provide the information of external resource.
 *
 * <p>Drivers that should be instantiated via a {@link ExternalResourceDriverFactory}.
 *
 * <p>TaskExecutor will retrieve the {@link ExternalResourceInfo} set of the external resource
 * from the drivers.
 */
@PublicEvolving
public interface ExternalResourceDriver {

	/**
	 * Retrieve the information of the external resources according to the amount.
	 *
	 * @param amount of the required external resources
	 * @return information set of the required external resources
	 * @throws Exception if there is something wrong during retrieving
	 */
	Set<? extends ExternalResourceInfo> retrieveResourceInfo(long amount) throws Exception;
}
