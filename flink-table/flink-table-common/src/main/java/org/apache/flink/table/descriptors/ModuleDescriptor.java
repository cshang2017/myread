package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.StringUtils;

import java.util.Map;

import static org.apache.flink.table.descriptors.ModuleDescriptorValidator.MODULE_TYPE;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Describes a {@link org.apache.flink.table.module.Module}.
 */
@PublicEvolving
public abstract class ModuleDescriptor extends DescriptorBase {

	private final String type;

	/**
	 * Constructs a {@link ModuleDescriptor}.
	 *
	 * @param type string that identifies this catalog
	 */
	public ModuleDescriptor(String type) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(type), "type cannot be null or empty");

		this.type = type;
	}

	@Override
	public final Map<String, String> toProperties() {
		final DescriptorProperties properties = new DescriptorProperties();
		properties.putString(MODULE_TYPE, type);

		properties.putProperties(toModuleProperties());
		return properties.asMap();
	}

	/**
	 * Converts this descriptor into a set of module properties.
	 */
	protected abstract Map<String, String> toModuleProperties();
}
