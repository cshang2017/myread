package org.apache.flink.table.planner.delegation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory to construct a {@link BatchPlanner} or {@link StreamPlanner}.
 */
@Internal
public final class BlinkPlannerFactory implements PlannerFactory {

	@Override
	public Planner create(
		Map<String, String> properties,
		Executor executor,
		TableConfig tableConfig,
		FunctionCatalog functionCatalog,
		CatalogManager catalogManager) {
		if (Boolean.valueOf(properties.getOrDefault(EnvironmentSettings.STREAMING_MODE, "true"))) {
			return new StreamPlanner(executor, tableConfig, functionCatalog, catalogManager);
		} else {
			return new BatchPlanner(executor, tableConfig, functionCatalog, catalogManager);
		}
	}

	@Override
	public Map<String, String> optionalContext() {
		Map<String, String> map = new HashMap<>();
		map.put(EnvironmentSettings.CLASS_NAME, this.getClass().getCanonicalName());
		return map;
	}

	@Override
	public Map<String, String> requiredContext() {
		DescriptorProperties properties = new DescriptorProperties();
		return properties.asMap();
	}

	@Override
	public List<String> supportedProperties() {
		return Arrays.asList(EnvironmentSettings.STREAMING_MODE, EnvironmentSettings.CLASS_NAME);
	}
}
