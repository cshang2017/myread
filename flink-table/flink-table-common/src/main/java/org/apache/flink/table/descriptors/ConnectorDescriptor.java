package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;

/**
 * Describes a connector to an other system.
 */
@PublicEvolving
public abstract class ConnectorDescriptor extends DescriptorBase implements Descriptor {

	private String type;
	private int version;
	private boolean formatNeeded;

	/**
	 * Constructs a {@link ConnectorDescriptor}.
	 *
	 * @param type string that identifies this connector
	 * @param version property version for backwards compatibility
	 * @param formatNeeded flag for basic validation of a needed format descriptor
	 */
	public ConnectorDescriptor(String type, int version, boolean formatNeeded) {
		this.type = type;
		this.version = version;
		this.formatNeeded = formatNeeded;
	}

	@Override
	public final Map<String, String> toProperties() {
		final DescriptorProperties properties = new DescriptorProperties();
		properties.putString(CONNECTOR_TYPE, type);
		properties.putLong(CONNECTOR_PROPERTY_VERSION, version);
		properties.putProperties(toConnectorProperties());
		return properties.asMap();
	}

	/**
	 * Returns if this connector requires a format descriptor.
	 */
	protected final boolean isFormatNeeded() {
		return formatNeeded;
	}

	/**
	 * Converts this descriptor into a set of connector properties. Usually prefixed with
	 * {@link ConnectorDescriptorValidator#CONNECTOR}.
	 */
	protected abstract Map<String, String> toConnectorProperties();
}
