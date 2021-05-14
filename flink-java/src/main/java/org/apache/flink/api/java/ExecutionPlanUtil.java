package org.apache.flink.api.java;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.Plan;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A utility for extracting an execution plan (as JSON) from a {@link Plan}.
 */
@Internal
public class ExecutionPlanUtil {

	private static final String PLAN_GENERATOR_CLASS_NAME = "org.apache.flink.optimizer.plandump.ExecutionPlanJSONGenerator";

	/**
	 * Extracts the execution plan (as JSON) from the given {@link Plan}.
	 */
	public static String getExecutionPlanAsJSON(Plan plan) {
		checkNotNull(plan);
		ExecutionPlanJSONGenerator jsonGenerator = getJSONGenerator();
		return jsonGenerator.getExecutionPlan(plan);
	}

	private static ExecutionPlanJSONGenerator getJSONGenerator() {
		Class<? extends ExecutionPlanJSONGenerator> planGeneratorClass = loadJSONGeneratorClass(
				PLAN_GENERATOR_CLASS_NAME);

			return planGeneratorClass.getConstructor().newInstance();
		
	}

	private static Class<? extends ExecutionPlanJSONGenerator> loadJSONGeneratorClass(String className) {
			Class<?> generatorClass = Class.forName(className);
			return generatorClass.asSubclass(ExecutionPlanJSONGenerator.class);
		
	}

	/**
	 * Internal interface for the JSON plan generator that has to reside in the optimizer package.
	 * We load the actual subclass using reflection.
	 */
	@Internal
	public interface ExecutionPlanJSONGenerator {

		/**
		 * Returns the execution plan as a JSON string.
		 */
		String getExecutionPlan(Plan plan);
	}
}
