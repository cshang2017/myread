package org.apache.flink.runtime.externalresource;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Static implementation of {@link ExternalResourceInfoProvider} which return fixed collection
 * of {@link ExternalResourceInfo}.
 */
public class StaticExternalResourceInfoProvider implements ExternalResourceInfoProvider {

	private final Map<String, Set<? extends ExternalResourceInfo>> externalResources;

	public StaticExternalResourceInfoProvider(Map<String, Set<? extends ExternalResourceInfo>> externalResources) {
		this.externalResources = externalResources;
	}

	@Override
	public Set<ExternalResourceInfo> getExternalResourceInfos(String resourceName) {
		if (!externalResources.containsKey(resourceName)) {
			return Collections.emptySet();
		}

		return Collections.unmodifiableSet(externalResources.get(resourceName));
	}

	@VisibleForTesting
	Map<String, Set<? extends ExternalResourceInfo>> getExternalResources() {
		return externalResources;
	}
}
