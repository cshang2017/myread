package org.apache.flink.runtime.jobmaster;

import org.apache.flink.util.AbstractID;

import javax.annotation.Nullable;

import java.util.UUID;

/**
 * The {@link JobMaster} fencing token.
 */
public class JobMasterId extends AbstractID {

	/**
	 * Creates a JobMasterId that takes the bits from the given UUID.
	 */
	public JobMasterId(UUID uuid) {
		super(uuid.getLeastSignificantBits(), uuid.getMostSignificantBits());
	}

	/**
	 * Generates a new random JobMasterId.
	 */
	private JobMasterId() {
		super();
	}

	/**
	 * Creates a UUID with the bits from this JobMasterId.
	 */
	public UUID toUUID() {
		return new UUID(getUpperPart(), getLowerPart());
	}

	/**
	 * Generates a new random JobMasterId.
	 */
	public static JobMasterId generate() {
		return new JobMasterId();
	}

	/**
	 * If the given uuid is null, this returns null, otherwise a JobMasterId that
	 * corresponds to the UUID, via {@link #JobMasterId(UUID)}.
	 */
	public static JobMasterId fromUuidOrNull(@Nullable UUID uuid) {
		return  uuid == null ? null : new JobMasterId(uuid);
	}
}
