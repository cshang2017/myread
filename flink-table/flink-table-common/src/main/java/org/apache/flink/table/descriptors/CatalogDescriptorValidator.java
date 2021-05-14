package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;

/**
 * Validator for {@link CatalogDescriptor}.
 */
@Internal
public abstract class CatalogDescriptorValidator implements DescriptorValidator {

	/**
	 * Key for describing the type of the catalog. Usually used for factory discovery.ca
	 */
	public static final String CATALOG_TYPE = "type";

	/**
	 * Key for describing the property version. This property can be used for backwards
	 * compatibility in case the property format changes.
	 */
	public static final String CATALOG_PROPERTY_VERSION = "property-version";

	/**
	 * Key for describing the default database of the catalog.
	 */
	public static final String CATALOG_DEFAULT_DATABASE = "default-database";

	@Override
	public void validate(DescriptorProperties properties) {
		properties.validateString(CATALOG_TYPE, false, 1);
		properties.validateInt(CATALOG_PROPERTY_VERSION, true, 0);
		properties.validateString(CATALOG_DEFAULT_DATABASE, true, 1);
	}
}
