package org.apache.flink.table.module;

import org.apache.flink.table.factories.ModuleFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.CoreModuleDescriptorValidator.MODULE_TYPE_CORE;
import static org.apache.flink.table.descriptors.ModuleDescriptorValidator.MODULE_TYPE;

/**
 * Factory for {@link CoreModule}.
 */
public class CoreModuleFactory implements ModuleFactory {

	@Override
	public Module createModule(Map<String, String> properties) {
		return CoreModule.INSTANCE;
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(MODULE_TYPE, MODULE_TYPE_CORE);

		return context;
	}

	@Override
	public List<String> supportedProperties() {
		return new ArrayList<>();
	}
}
