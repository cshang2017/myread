
package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;

import java.util.Map;

/**
 * Describes a custom connector to an other system.
 */
@Internal
public class CustomConnectorDescriptor extends ConnectorDescriptor {

	private final DescriptorProperties properties;

	/**
	 * Constructs a {@link CustomConnectorDescriptor}.
	 *
	 * @param type String that identifies this connector.
	 * @param version Property version for backwards compatibility.
	 * @param formatNeeded Flag for basic validation of a needed format descriptor.
	 */
	public CustomConnectorDescriptor(String type, int version, boolean formatNeeded) {
		super(type, version, formatNeeded);
		properties = new DescriptorProperties();
	}

	/**
	 * Adds a configuration property for the connector.
	 *
	 * @param key The property key to be set.
	 * @param value The property value to be set.
	 */
	public CustomConnectorDescriptor property(String key, String value) {
		properties.putString(key, value);
		return this;
	}

	/**
	 * Adds a set of properties for the connector.
	 *
	 * @param properties The properties to add.
	 */
	public CustomConnectorDescriptor properties(Map<String, String> properties) {
		this.properties.putProperties(properties);
		return this;
	}

	@Override
	protected Map<String, String> toConnectorProperties() {
		return properties.asMap();
	}
}
