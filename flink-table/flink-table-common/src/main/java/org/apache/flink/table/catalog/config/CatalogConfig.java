package org.apache.flink.table.catalog.config;

/**
 * Config for catalog and catalog meta-objects.
 */
public class CatalogConfig {

	/**
	 * Flag to distinguish if a meta-object is generic Flink object or not.
	 */
	public static final String IS_GENERIC = "is_generic";

	// Globally reserved prefix for catalog properties.
	// User defined properties should not with this prefix.
	// Used to distinguish properties created by Hive and Flink,
	// as Hive metastore has its own properties created upon table creation and migration between different versions of metastore.
	public static final String FLINK_PROPERTY_PREFIX = "flink.";
}
