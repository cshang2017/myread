package org.apache.flink.table.catalog;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A implementation of {@link CatalogDatabase}.
 */
public class CatalogDatabaseImpl implements CatalogDatabase {
	// Property of the database
	private final Map<String, String> properties;
	// Comment of the database
	private final String comment;

	public CatalogDatabaseImpl(Map<String, String> properties, @Nullable String comment) {
		this.properties = checkNotNull(properties, "properties cannot be null");
		this.comment = comment;
	}

	/**
	 * Get a map of properties associated with the database.
	 */
	public Map<String, String> getProperties() {
		return properties;
	}

	/**
	 * Get comment of the database.
	 *
	 * @return comment of the database
	 */
	public String getComment() {
		return comment;
	}

	/**
	 * Get a deep copy of the CatalogDatabase instance.
	 *
	 * @return a copy of CatalogDatabase instance
	 */
	public CatalogDatabase copy() {
		return new CatalogDatabaseImpl(new HashMap<>(properties), comment);
	}

	/**
	 * Get a brief description of the database.
	 *
	 * @return an optional short description of the database
	 */
	public Optional<String> getDescription() {
		return Optional.ofNullable(comment);
	}

	/**
	 * Get a detailed description of the database.
	 *
	 * @return an optional long description of the database
	 */
	public Optional<String> getDetailedDescription() {
		return Optional.ofNullable(comment);
	}
}
