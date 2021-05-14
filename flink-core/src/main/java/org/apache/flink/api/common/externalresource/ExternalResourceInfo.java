package org.apache.flink.api.common.externalresource;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Collection;
import java.util.Optional;

/**
 * Contains the information of an external resource.
 */
@PublicEvolving
public interface ExternalResourceInfo {

	/**
	 * Get the property indicated by the specified key.
	 *
	 * @param key of the required property
	 * @return an {@code Optional} containing the value associated to the key, or an empty {@code Optional} if no value has been stored under the given key
	 */
	Optional<String> getProperty(String key);

	/**
	 * Get all property keys.
	 *
	 * @return collection of all property keys
	 */
	Collection<String> getKeys();
}
