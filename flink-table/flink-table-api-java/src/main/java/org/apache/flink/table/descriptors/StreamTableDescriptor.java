

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.internal.Registration;

/**
 * Describes a table connected from a streaming environment.
 *
 * <p>This class just exists for backwards compatibility use {@link ConnectTableDescriptor} for
 * declarations.
 */
@PublicEvolving
public final class StreamTableDescriptor extends ConnectTableDescriptor {

	public StreamTableDescriptor(Registration registration, ConnectorDescriptor connectorDescriptor) {
		super(registration, connectorDescriptor);
	}

	@Override
	public StreamTableDescriptor withSchema(Schema schema) {
		return (StreamTableDescriptor) super.withSchema(schema);
	}
}
