package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Map;

import static org.apache.flink.table.descriptors.FileSystemValidator.CONNECTOR_PATH;
import static org.apache.flink.table.descriptors.FileSystemValidator.CONNECTOR_TYPE_VALUE;

/**
 * Connector descriptor for a file system.
 */
@PublicEvolving
public class FileSystem extends ConnectorDescriptor {

	private String path = null;

	public FileSystem() {
		super(CONNECTOR_TYPE_VALUE, 1, true);
	}

	/**
	 * Sets the path to a file or directory in a file system.
	 *
	 * @param path the path a file or directory
	 */
	public FileSystem path(String path) {
		this.path = path;
		return this;
	}

	@Override
	protected Map<String, String> toConnectorProperties() {
		DescriptorProperties properties = new DescriptorProperties();
		if (path != null) {
			properties.putString(CONNECTOR_PATH, path);
		}
		return properties.asMap();
	}
}
