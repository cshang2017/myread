package org.apache.flink.table.descriptors.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptor;

import java.util.Map;

@Internal
public class CustomFormatDescriptor extends FormatDescriptor {

	private final DescriptorProperties properties;

	/**
	 * Constructs a {@link CustomFormatDescriptor}.
	 *
	 * @param type String that identifies this format.
	 * @param version Property version for backwards compatibility.
	 */
	public CustomFormatDescriptor(String type, int version) {
		super(type, version);
		properties = new DescriptorProperties();
	}

	/**
	 * Adds a configuration property for the format.
	 *
	 * @param key The property key to be set.
	 * @param value The property value to be set.
	 */
	public CustomFormatDescriptor property(String key, String value) {
		properties.putString(key, value);
		return this;
	}

	/**
	 * Adds a set of properties for the format.
	 *
	 * @param properties The properties to add.
	 */
	public CustomFormatDescriptor properties(Map<String, String> properties) {
		this.properties.putProperties(properties);
		return this;
	}

	@Override
	protected Map<String, String> toFormatProperties() {
		return properties.asMap();
	}
}
