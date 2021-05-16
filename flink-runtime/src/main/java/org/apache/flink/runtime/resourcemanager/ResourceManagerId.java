

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.util.AbstractID;

import javax.annotation.Nullable;

import java.util.UUID;

/**
 * Fencing token for the {@link ResourceManager}.
 */
public class ResourceManagerId extends AbstractID {

	/**
	 * Generates a new random ResourceManagerId.
	 */
	private ResourceManagerId() {}

	/**
	 * Creates a ResourceManagerId that takes the bits from the given UUID.
	 */
	private ResourceManagerId(UUID uuid) {
		super(uuid.getLeastSignificantBits(), uuid.getMostSignificantBits());
	}

	/**
	 * Creates a UUID with the bits from this ResourceManagerId.
	 */
	public UUID toUUID() {
		return new UUID(getUpperPart(), getLowerPart());
	}

	/**
	 * Generates a new random ResourceManagerId.
	 */
	public static ResourceManagerId generate() {
		return new ResourceManagerId();
	}

	/**
	 * Creates a ResourceManagerId that corresponds to the given UUID.
	 */
	public static ResourceManagerId fromUuid(UUID uuid) {
		return new ResourceManagerId(uuid);
	}

	/**
	 * If the given uuid is null, this returns null, otherwise a ResourceManagerId that
	 * corresponds to the UUID, via {@link #ResourceManagerId(UUID)}.
	 */
	public static ResourceManagerId fromUuidOrNull(@Nullable UUID uuid) {
		return  uuid == null ? null : new ResourceManagerId(uuid);
	}
}
