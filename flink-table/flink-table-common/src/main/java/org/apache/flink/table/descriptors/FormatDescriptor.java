package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Map;

/**
 * Describes the format of data.
 */
@PublicEvolving
public abstract class FormatDescriptor extends DescriptorBase implements Descriptor {

	private String type;
	private int version;

	/**
	 * Constructs a {@link FormatDescriptor}.
	 *
	 * @param type string that identifies this format
	 * @param version property version for backwards compatibility
	 */
	public FormatDescriptor(String type, int version) {
		this.type = type;
		this.version = version;
	}

	@Override
	public final Map<String, String> toProperties() {
		final DescriptorProperties properties = new DescriptorProperties();
		properties.putString(FormatDescriptorValidator.FORMAT_TYPE, type);
		properties.putInt(FormatDescriptorValidator.FORMAT_PROPERTY_VERSION, version);
		properties.putProperties(toFormatProperties());
		return properties.asMap();
	}

	/**
	 * Converts this descriptor into a set of format properties. Usually prefixed with
	 * {@link FormatDescriptorValidator#FORMAT}.
	 */
	protected abstract Map<String, String> toFormatProperties();
}
