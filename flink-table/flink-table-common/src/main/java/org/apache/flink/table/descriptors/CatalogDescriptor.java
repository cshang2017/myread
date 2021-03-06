package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.StringUtils;

import java.util.Map;

import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Describes a catalog of tables, views, and functions.
 */
@PublicEvolving
public abstract class CatalogDescriptor extends DescriptorBase {

	private final String type;

	private final int propertyVersion;

	private final String defaultDatabase;

	/**
	 * Constructs a {@link CatalogDescriptor}.
	 *
	 * @param type string that identifies this catalog
	 * @param propertyVersion property version for backwards compatibility
	 */
	public CatalogDescriptor(String type, int propertyVersion) {
		this(type, propertyVersion, null);
	}

	/**
	 * Constructs a {@link CatalogDescriptor}.
	 *
	 * @param type string that identifies this catalog
	 * @param propertyVersion property version for backwards compatibility
	 * @param defaultDatabase default database of the catalog
	 */
	public CatalogDescriptor(String type, int propertyVersion, String defaultDatabase) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(type), "type cannot be null or empty");

		this.type = type;
		this.propertyVersion = propertyVersion;
		this.defaultDatabase = defaultDatabase;
	}

	@Override
	public final Map<String, String> toProperties() {
		final DescriptorProperties properties = new DescriptorProperties();
		properties.putString(CATALOG_TYPE, type);
		properties.putLong(CATALOG_PROPERTY_VERSION, propertyVersion);

		if (defaultDatabase != null) {
			properties.putString(CATALOG_DEFAULT_DATABASE, defaultDatabase);
		}

		properties.putProperties(toCatalogProperties());
		return properties.asMap();
	}

	public String getDefaultDatabase() {
		return defaultDatabase;
	}

	/**
	 * Converts this descriptor into a set of catalog properties.
	 */
	protected abstract Map<String, String> toCatalogProperties();
}
