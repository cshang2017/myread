

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.util.AbstractID;

import java.util.UUID;

/**
 * Fencing token of the {@link Dispatcher}.
 */
public class DispatcherId extends AbstractID {

	private DispatcherId() {}

	private DispatcherId(UUID uuid) {
		super(uuid.getLeastSignificantBits(), uuid.getMostSignificantBits());
	}

	public UUID toUUID() {
		return new UUID(getUpperPart(), getLowerPart());
	}

	public static DispatcherId generate() {
		return new DispatcherId();
	}

	public static DispatcherId fromUuid(UUID uuid) {
		return new DispatcherId(uuid);
	}
}
