package org.apache.flink.table.catalog;

import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A database name and object (table/view/function) name combo in a catalog.
 */
public class ObjectPath implements Serializable {
	private final String databaseName;
	private final String objectName;

	public ObjectPath(String databaseName, String objectName) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(objectName), "objectName cannot be null or empty");

		this.databaseName = databaseName;
		this.objectName = objectName;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public String getObjectName() {
		return objectName;
	}

	public String getFullName() {
		return String.format("%s.%s", databaseName, objectName);
	}

	public static ObjectPath fromString(String fullName) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(fullName), "fullName cannot be null or empty");

		String[] paths = fullName.split("\\.");

		if (paths.length != 2) {
			throw new IllegalArgumentException(
				String.format("Cannot get split '%s' to get databaseName and objectName", fullName));
		}

		return new ObjectPath(paths[0], paths[1]);
	}
	
	@Override
	public String toString() {
		return String.format("%s.%s", databaseName, objectName);
	}

}
