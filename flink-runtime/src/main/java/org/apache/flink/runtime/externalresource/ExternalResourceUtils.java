package org.apache.flink.runtime.externalresource;

import org.apache.flink.api.common.externalresource.ExternalResourceDriver;
import org.apache.flink.api.common.externalresource.ExternalResourceDriverFactory;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.ExternalResourceOptions;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Utility class for external resource framework.
 */
public class ExternalResourceUtils {


	private ExternalResourceUtils() {
		throw new UnsupportedOperationException("This class should never be instantiated.");
	}

	/**
	 * Get the enabled external resource list from configuration.
	 */
	private static Set<String> getExternalResourceSet(Configuration config) {
		return new HashSet<>(config.get(ExternalResourceOptions.EXTERNAL_RESOURCE_LIST));
	}

	/**
	 * Get the external resources map. The key should be used for deployment specific container request,
	 * and values should be the amount of that resource.
	 *
	 * @param config Configurations
	 * @param suffix suffix of config option for deployment specific configuration key
	 * @return external resources map, map the amount to the configuration key for deployment specific container request
	 */
	public static Map<String, Long> getExternalResources(Configuration config, String suffix) {
		final Set<String> resourceSet = getExternalResourceSet(config);

		if (resourceSet.isEmpty()) {
			return Collections.emptyMap();
		}

		final Map<String, Long> externalResourceConfigs = new HashMap<>();
		for (String resourceName: resourceSet) {
			final ConfigOption<Long> amountOption =
				key(ExternalResourceOptions.getAmountConfigOptionForResource(resourceName))
					.longType()
					.noDefaultValue();
			final ConfigOption<String> configKeyOption =
				key(ExternalResourceOptions.getSystemConfigKeyConfigOptionForResource(resourceName, suffix))
					.stringType()
					.noDefaultValue();
			final String configKey = config.getString(configKeyOption);
			final Optional<Long> amountOpt = config.getOptional(amountOption);

			if (StringUtils.isNullOrWhitespaceOnly(configKey)) {
				continue;
			}
			if (!amountOpt.isPresent()) {
				continue;
			} else if (amountOpt.get() <= 0) {
				continue;
			}

			if (externalResourceConfigs.put(configKey, amountOpt.get()) != null) {
			} else {
			}
		}

		return externalResourceConfigs;
	}

	/**
	 * Get the map of resource name and amount of all of enabled external resources.
	 */
	public static Map<String, Long> getExternalResourceAmountMap(Configuration config) {
		final Set<String> resourceSet = getExternalResourceSet(config);

		if (resourceSet.isEmpty()) {
			return Collections.emptyMap();
		}

		final Map<String, Long> externalResourceAmountMap = new HashMap<>();
		for (String resourceName: resourceSet) {
			final ConfigOption<Long> amountOption =
				key(ExternalResourceOptions.getAmountConfigOptionForResource(resourceName))
					.longType()
					.noDefaultValue();
			final Optional<Long> amountOpt = config.getOptional(amountOption);
			if (!amountOpt.isPresent()) {
			} else if (amountOpt.get() <= 0) {
			} else {
				externalResourceAmountMap.put(resourceName, amountOpt.get());
			}
		}

		return externalResourceAmountMap;
	}

	/**
	 * Instantiate the {@link ExternalResourceDriver ExternalResourceDrivers} for all of enabled external resources. {@link ExternalResourceDriver ExternalResourceDrivers}
	 * are mapped to its resource name.
	 */
	public static Map<String, ExternalResourceDriver> externalResourceDriversFromConfig(Configuration config, PluginManager pluginManager) {
		final Set<String> resourceSet = getExternalResourceSet(config);

		if (resourceSet.isEmpty()) {
			return Collections.emptyMap();
		}

		final Iterator<ExternalResourceDriverFactory> factoryIterator = pluginManager.load(ExternalResourceDriverFactory.class);
		final Map<String, ExternalResourceDriverFactory> externalResourceFactories = new HashMap<>();
		factoryIterator.forEachRemaining(
			externalResourceDriverFactory -> externalResourceFactories.put(externalResourceDriverFactory.getClass().getName(), externalResourceDriverFactory));

		final Map<String, ExternalResourceDriver> externalResourceDrivers = new HashMap<>();
		for (String resourceName: resourceSet) {
			final ConfigOption<String> driverClassOption =
				key(ExternalResourceOptions.getExternalResourceDriverFactoryConfigOptionForResource(resourceName))
					.stringType()
					.noDefaultValue();
			final String driverFactoryClassName = config.getString(driverClassOption);
			if (StringUtils.isNullOrWhitespaceOnly(driverFactoryClassName)) {
				continue;
			}

			ExternalResourceDriverFactory externalResourceDriverFactory = externalResourceFactories.get(driverFactoryClassName);
			if (externalResourceDriverFactory != null) {
				DelegatingConfiguration delegatingConfiguration =
					new DelegatingConfiguration(config, ExternalResourceOptions.getExternalResourceParamConfigPrefixForResource(resourceName));
					externalResourceDrivers.put(resourceName, externalResourceDriverFactory.createExternalResourceDriver(delegatingConfiguration));
			}
		}

		return externalResourceDrivers;
	}

	/**
	 * Instantiate {@link StaticExternalResourceInfoProvider} for all of enabled external resources.
	 */
	public static ExternalResourceInfoProvider createStaticExternalResourceInfoProvider(Map<String, Long> externalResourceAmountMap, Map<String, ExternalResourceDriver> externalResourceDrivers) {
		final Map<String, Set<? extends ExternalResourceInfo>> externalResources = new HashMap<>();
		for (Map.Entry<String, ExternalResourceDriver> externalResourceDriverEntry : externalResourceDrivers.entrySet()) {
			final String resourceName = externalResourceDriverEntry.getKey();
			final ExternalResourceDriver externalResourceDriver = externalResourceDriverEntry.getValue();
			if (externalResourceAmountMap.containsKey(resourceName)) {
					final Set<? extends ExternalResourceInfo> externalResourceInfos;
					externalResourceInfos = externalResourceDriver.retrieveResourceInfo(externalResourceAmountMap.get(resourceName));
					externalResources.put(resourceName, externalResourceInfos);
			}
		}
		return new StaticExternalResourceInfoProvider(externalResources);
	}
}
