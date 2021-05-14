package org.apache.flink.table.module;

import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Module of default core metadata in Flink.
 */
public class CoreModule implements Module {
	public static final CoreModule INSTANCE = new CoreModule();

	private CoreModule() {
	}

	@Override
	public Set<String> listFunctions() {
		return BuiltInFunctionDefinitions.getDefinitions()
			.stream()
			.map(f -> f.getName())
			.collect(Collectors.toSet());
	}

	@Override
	public Optional<FunctionDefinition> getFunctionDefinition(String name) {
		return BuiltInFunctionDefinitions.getDefinitions().stream()
				.filter(f -> f.getName().equalsIgnoreCase(name))
				.findFirst()
				.map(Function.identity());
	}
}
