package org.apache.flink.table.descriptors;

/**
 * Validator for {@link GenericInMemoryCatalogDescriptor}.
 */
public class GenericInMemoryCatalogValidator extends CatalogDescriptorValidator {
	public static final String CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY = "generic_in_memory";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CATALOG_TYPE, CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY, false);
	}
}
