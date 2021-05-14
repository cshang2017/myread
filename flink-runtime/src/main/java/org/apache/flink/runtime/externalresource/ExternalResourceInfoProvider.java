package org.apache.flink.runtime.externalresource;

import org.apache.flink.api.common.externalresource.ExternalResourceInfo;

import java.util.Collections;
import java.util.Set;

/**
 * Provide the information of external resources.
 */
public interface ExternalResourceInfoProvider {

	ExternalResourceInfoProvider NO_EXTERNAL_RESOURCES = resourceName -> Collections.emptySet();

	/**
	 * Get the specific external resource information by the resourceName.
	 *
	 * @param resourceName of the required external resource
	 * @return information set of the external resource identified by the resourceName
	 */
	Set<ExternalResourceInfo> getExternalResourceInfos(String resourceName);
}
