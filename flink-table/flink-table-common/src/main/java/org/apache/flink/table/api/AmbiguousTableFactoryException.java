package org.apache.flink.table.api;

import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Exception for finding more than one {@link TableFactory} for the given properties.
 */
public class AmbiguousTableFactoryException extends RuntimeException {

	// factories that match the properties
	private final List<? extends TableFactory> matchingFactories;
	// required factory class
	private final Class<? extends TableFactory> factoryClass;
	// all found factories
	private final List<TableFactory> factories;
	// properties that describe the configuration
	private final Map<String, String> properties;

	public AmbiguousTableFactoryException(
			List<? extends TableFactory> matchingFactories,
			Class<? extends TableFactory> factoryClass,
			List<TableFactory> factories,
			Map<String, String> properties,
			Throwable cause) {

		super(cause);
		this.matchingFactories = matchingFactories;
		this.factoryClass = factoryClass;
		this.factories = factories;
		this.properties = properties;
	}

	public AmbiguousTableFactoryException(
			List<? extends TableFactory> matchingFactories,
			Class<? extends TableFactory> factoryClass,
			List<TableFactory> factories,
			Map<String, String> properties) {

		this(matchingFactories, factoryClass, factories, properties, null);
	}

	@Override
	public String getMessage() {
		return String.format(
			"More than one suitable table factory for '%s' could be found in the classpath.\n\n" +
				"The following factories match:\n%s\n\n" +
				"The following properties are requested:\n%s\n\n" +
				"The following factories have been considered:\n%s",
			factoryClass.getName(),
			matchingFactories.stream()
				.map(p -> p.getClass().getName())
				.collect(Collectors.joining("\n")),
			DescriptorProperties.toString(properties),
			factories.stream()
				.map(p -> p.getClass().getName())
				.collect(Collectors.joining("\n"))
		);
	}
}
