package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.resourcemanager.ResourceManagerId;

import javax.annotation.Nonnull;

import java.util.Objects;

/**
 * Current address and fencing token of the leading ResourceManager.
 */
public class ResourceManagerAddress {

	@Nonnull
	private final String address;

	@Nonnull
	private final ResourceManagerId resourceManagerId;

	public ResourceManagerAddress(@Nonnull String address, @Nonnull ResourceManagerId resourceManagerId) {
		this.address = address;
		this.resourceManagerId = resourceManagerId;
	}

	@Nonnull
	public String getAddress() {
		return address;
	}

	@Nonnull
	public ResourceManagerId getResourceManagerId() {
		return resourceManagerId;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}

		ResourceManagerAddress that = (ResourceManagerAddress) obj;
		return Objects.equals(address, that.address) &&
			Objects.equals(resourceManagerId, that.resourceManagerId);
	}

	@Override
	public int hashCode() {
		return Objects.hash(address, resourceManagerId);
	}

	@Override
	public String toString() {
		return address + '(' + resourceManagerId + ')';
	}
}
