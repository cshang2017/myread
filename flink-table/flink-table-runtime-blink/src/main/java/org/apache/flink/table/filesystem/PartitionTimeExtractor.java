package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.Experimental;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Time extractor to extract time from partition values.
 */
@Experimental
public interface PartitionTimeExtractor extends Serializable {

	String DEFAULT = "default";
	String CUSTOM = "custom";

	/**
	 * Extract time from partition keys and values.
	 */
	LocalDateTime extract(List<String> partitionKeys, List<String> partitionValues);

	static PartitionTimeExtractor create(
			ClassLoader userClassLoader,
			String extractorKind,
			String extractorClass,
			String extractorPattern) {
		switch (extractorKind) {
			case DEFAULT:
				return new DefaultPartTimeExtractor(extractorPattern);
			case CUSTOM:
				try {
					return (PartitionTimeExtractor) userClassLoader.loadClass(extractorClass).newInstance();
				} catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
					throw new RuntimeException(
							"Can not new instance for custom class from " + extractorClass, e);
				}
			default:
				throw new UnsupportedOperationException(
						"Unsupported extractor kind: " + extractorKind);
		}
	}
}
