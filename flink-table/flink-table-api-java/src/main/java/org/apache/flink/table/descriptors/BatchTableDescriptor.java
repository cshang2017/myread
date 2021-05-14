

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.internal.Registration;

/**
 * Describes a table connected from a batch environment.
 *
 * <p>This class just exists for backwards compatibility use {@link ConnectTableDescriptor} for
 * declarations.
 */
@PublicEvolving
public final class BatchTableDescriptor extends ConnectTableDescriptor {

	public BatchTableDescriptor(Registration registration, ConnectorDescriptor connectorDescriptor) {
		super(registration, connectorDescriptor);
	}

	@Override
	public BatchTableDescriptor withSchema(Schema schema) {
		return (BatchTableDescriptor) super.withSchema(schema);
	}
}
