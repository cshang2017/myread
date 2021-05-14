package org.apache.flink.table.factories;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.AmbiguousTableFactoryException;
import org.apache.flink.table.api.NoMatchingTableFactoryException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Unified class to search for a {@link ComponentFactory} of provided type and properties. It is similar to
 * {@link TableFactoryService} but it disambiguates based on {@link ComponentFactory#optionalContext()}.
 */
@Internal
public class ComponentFactoryService {

	/**
	 * Finds a table factory of the given class and property map. This method enables
	 * disambiguating multiple matching {@link ComponentFactory}s based on additional
	 * optional context provided via {@link ComponentFactory#optionalContext()}.
	 *
	 * @param factoryClass desired factory class
	 * @param propertyMap properties that describe the factory configuration
	 * @param <T> factory class type
	 * @return the matching factory
	 */
	public static <T extends ComponentFactory> T find(Class<T> factoryClass, Map<String, String> propertyMap) {
		List<T> all = TableFactoryService.findAll(factoryClass, propertyMap);

		List<T> filtered = all.stream().filter(factory -> {
			Map<String, String> optionalContext = factory.optionalContext();
			return optionalContext.entrySet().stream().allMatch(entry -> {
					String property = propertyMap.get(entry.getKey());
					if (property != null) {
						return property.equals(entry.getValue());
					} else {
						return true;
					}
				}
			);
		}).collect(Collectors.toList());

		if (filtered.size() > 1) {
			throw new AmbiguousTableFactoryException(
				filtered,
				factoryClass,
				new ArrayList<>(all),
				propertyMap
			);
		} else if (filtered.isEmpty()) {
			throw new NoMatchingTableFactoryException(
				"No factory supports the additional filters.",
				factoryClass,
				new ArrayList<>(all),
				propertyMap);
		} else {
			return filtered.get(0);
		}
	}
}
