package org.apache.flink.table.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig.ClosureCleanerLevel;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.InstantiationUtil;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.getAllDeclaredMethods;

/**
 * Utility for dealing with subclasses of {@link UserDefinedFunction}. The purpose of this class is to
 * keep the user-facing APIs clean and offer methods/constants from here.
 *
 * <p>It contains methods for instantiating, validating and extracting types during function registration
 * in a catalog.
 */
@Internal
public final class UserDefinedFunctionHelper {

	// method names of code generated UDFs

	public static final String SCALAR_EVAL = "eval";
	public static final String TABLE_EVAL = "eval";

	public static final String AGGREGATE_ACCUMULATE = "accumulate";
	public static final String AGGREGATE_RETRACT = "retract";
	public static final String AGGREGATE_MERGE = "merge";
	public static final String AGGREGATE_RESET = "resetAccumulator";

	public static final String TABLE_AGGREGATE_ACCUMULATE = "accumulate";
	public static final String TABLE_AGGREGATE_RETRACT = "retract";
	public static final String TABLE_AGGREGATE_EMIT = "emitValue";
	public static final String TABLE_AGGREGATE_EMIT_RETRACT = "emitUpdateWithRetract";

	public static final String ASYNC_TABLE_EVAL = "eval";

	/**
	 * Tries to infer the TypeInformation of an AggregateFunction's accumulator type.
	 *
	 * @param aggregateFunction The AggregateFunction for which the accumulator type is inferred.
	 * @return The inferred accumulator type of the AggregateFunction.
	 */
	public static <T, ACC> TypeInformation<T> getReturnTypeOfAggregateFunction(
			UserDefinedAggregateFunction<T, ACC> aggregateFunction) {
		return getReturnTypeOfAggregateFunction(aggregateFunction, null);
	}

	/**
	 * Tries to infer the TypeInformation of an AggregateFunction's accumulator type.
	 *
	 * @param aggregateFunction The AggregateFunction for which the accumulator type is inferred.
	 * @param scalaType The implicitly inferred type of the accumulator type.
	 *
	 * @return The inferred accumulator type of the AggregateFunction.
	 */
	public static <T, ACC> TypeInformation<T> getReturnTypeOfAggregateFunction(
			UserDefinedAggregateFunction<T, ACC> aggregateFunction,
			TypeInformation<T> scalaType) {

		TypeInformation<T> userProvidedType = aggregateFunction.getResultType();
		if (userProvidedType != null) {
			return userProvidedType;
		} else if (scalaType != null) {
			return scalaType;
		} else {
			return TypeExtractor.createTypeInfo(
				aggregateFunction,
				UserDefinedAggregateFunction.class,
				aggregateFunction.getClass(),
				0);
		}
	}

	/**
	 * Tries to infer the TypeInformation of an AggregateFunction's accumulator type.
	 *
	 * @param aggregateFunction The AggregateFunction for which the accumulator type is inferred.
	 * @return The inferred accumulator type of the AggregateFunction.
	 */
	public static <T, ACC> TypeInformation<ACC> getAccumulatorTypeOfAggregateFunction(
			UserDefinedAggregateFunction<T, ACC> aggregateFunction) {
		return getAccumulatorTypeOfAggregateFunction(aggregateFunction, null);
	}

	/**
	 * Tries to infer the TypeInformation of an AggregateFunction's accumulator type.
	 *
	 * @param aggregateFunction The AggregateFunction for which the accumulator type is inferred.
	 * @param scalaType The implicitly inferred type of the accumulator type.
	 *
	 * @return The inferred accumulator type of the AggregateFunction.
	 */
	public static <T, ACC> TypeInformation<ACC> getAccumulatorTypeOfAggregateFunction(
			UserDefinedAggregateFunction<T, ACC> aggregateFunction,
			TypeInformation<ACC> scalaType) {

		TypeInformation<ACC> userProvidedType = aggregateFunction.getAccumulatorType();
		if (userProvidedType != null) {
			return userProvidedType;
		} else if (scalaType != null) {
			return scalaType;
		} else {
			return TypeExtractor.createTypeInfo(
				aggregateFunction,
				UserDefinedAggregateFunction.class,
				aggregateFunction.getClass(),
				1);
		}
	}

	/**
	 * Tries to infer the TypeInformation of an AggregateFunction's accumulator type.
	 *
	 * @param tableFunction The TableFunction for which the accumulator type is inferred.
	 * @return The inferred accumulator type of the AggregateFunction.
	 */
	public static <T> TypeInformation<T> getReturnTypeOfTableFunction(TableFunction<T> tableFunction) {
		return getReturnTypeOfTableFunction(tableFunction, null);
	}

	/**
	 * Tries to infer the TypeInformation of an AggregateFunction's accumulator type.
	 *
	 * @param tableFunction The TableFunction for which the accumulator type is inferred.
	 * @param scalaType The implicitly inferred type of the accumulator type.
	 * @return The inferred accumulator type of the AggregateFunction.
	 */
	public static <T> TypeInformation<T> getReturnTypeOfTableFunction(
			TableFunction<T> tableFunction,
			TypeInformation<T> scalaType) {

		TypeInformation<T> userProvidedType = tableFunction.getResultType();
		if (userProvidedType != null) {
			return userProvidedType;
		} else if (scalaType != null) {
			return scalaType;
		} else {
			return TypeExtractor.createTypeInfo(
				tableFunction,
				TableFunction.class,
				tableFunction.getClass(),
				0);
		}
	}

	/**
	 * Instantiates a {@link UserDefinedFunction} assuming a default constructor.
	 */
	public static UserDefinedFunction instantiateFunction(Class<? extends UserDefinedFunction> functionClass) {
		validateClass(functionClass, true);
		try {
			return functionClass.newInstance();
		} catch (Exception e) {
			throw new ValidationException(
				String.format("Cannot instantiate user-defined function class '%s'.", functionClass.getName()),
				e);
		}
	}

	/**
	 * Prepares a {@link UserDefinedFunction} instance for usage in the API.
	 */
	public static void prepareInstance(ReadableConfig config, UserDefinedFunction function) {
		validateClass(function.getClass(), false);
		cleanFunction(config, function);
	}

	/**
	 * Validates a {@link UserDefinedFunction} class for usage in the API.
	 *
	 * <p>Note: This is an initial validation to indicate common errors early. The concrete signature
	 * validation happens in the code generation when the actual {@link DataType}s for arguments and result
	 * are known.
	 */
	public static void validateClass(Class<? extends UserDefinedFunction> functionClass) {
		validateClass(functionClass, true);
	}

	/**
	 * Validates a {@link UserDefinedFunction} class for usage in the API.
	 */
	private static void validateClass(
			Class<? extends UserDefinedFunction> functionClass,
			boolean requiresDefaultConstructor) {
		if (TableFunction.class.isAssignableFrom(functionClass)) {
			validateNotSingleton(functionClass);
		}
		validateInstantiation(functionClass, requiresDefaultConstructor);
		validateImplementationMethods(functionClass);
	}

	/**
	 * Check whether this is a Scala object. Using Scala objects can lead to concurrency issues,
	 * e.g., due to a shared collector.
	 */
	private static void validateNotSingleton(Class<?> clazz) {
		if (Arrays.stream(clazz.getFields()).anyMatch(f -> f.getName().equals("MODULE$"))) {
			throw new ValidationException(String.format(
				"Function implemented by class %s is a Scala object. This is forbidden because of concurrency" +
					" problems when using them.", clazz.getName()));
		}
	}

	/**
	 * Validates the implementation methods such as {@link #SCALAR_EVAL} or {@link #AGGREGATE_ACCUMULATE}
	 * depending on the {@link UserDefinedFunction} subclass.
	 *
	 * <p>This method must be kept in sync with the code generation requirements and the individual
	 * docs of each function.
	 */
	private static void validateImplementationMethods(Class<? extends UserDefinedFunction> functionClass) {
		if (ScalarFunction.class.isAssignableFrom(functionClass)) {
			validateImplementationMethod(functionClass, false, false, SCALAR_EVAL);
		} else if (TableFunction.class.isAssignableFrom(functionClass)) {
			validateImplementationMethod(functionClass, true, false, TABLE_EVAL);
		} else if (AggregateFunction.class.isAssignableFrom(functionClass)) {
			validateImplementationMethod(functionClass, true, false, AGGREGATE_ACCUMULATE);
			validateImplementationMethod(functionClass, true, true, AGGREGATE_RETRACT);
			validateImplementationMethod(functionClass, true, true, AGGREGATE_MERGE);
			validateImplementationMethod(functionClass, true, true, AGGREGATE_RESET);
		} else if (TableAggregateFunction.class.isAssignableFrom(functionClass)) {
			validateImplementationMethod(functionClass, true, false, TABLE_AGGREGATE_ACCUMULATE);
			validateImplementationMethod(functionClass, true, true, TABLE_AGGREGATE_RETRACT);
			validateImplementationMethod(functionClass, true, false, TABLE_AGGREGATE_EMIT, TABLE_AGGREGATE_EMIT_RETRACT);
		}
	}

	/**
	 * Validates an implementation method such as {@code eval()} or {@code accumulate()}.
	 */
	private static void validateImplementationMethod(
			Class<? extends UserDefinedFunction> clazz,
			boolean rejectStatic,
			boolean isOptional,
			String... methodNameOptions) {
		final Set<String> nameSet = new HashSet<>(Arrays.asList(methodNameOptions));
		final List<Method> methods = getAllDeclaredMethods(clazz);
		boolean found = false;
		for (Method method : methods) {
			if (!nameSet.contains(method.getName())) {
				continue;
			}
			found = true;
			final int modifier = method.getModifiers();
			if (!Modifier.isPublic(modifier)) {
				throw new ValidationException(
					String.format(
						"Method '%s' of function class '%s' is not public.",
						method.getName(),
						clazz.getName()));
			}
			if (Modifier.isAbstract(modifier)) {
				throw new ValidationException(
					String.format(
						"Method '%s' of function class '%s' must not be abstract.",
						method.getName(),
						clazz.getName()));
			}
			if (rejectStatic && Modifier.isStatic(modifier)) {
				throw new ValidationException(
					String.format(
						"Method '%s' of function class '%s' must not be static.",
						method.getName(),
						clazz.getName()));
			}
		}
		if (!found && !isOptional) {
			throw new ValidationException(
				String.format(
					"Function class '%s' does not implement a method named %s.",
					clazz.getName(),
					nameSet.stream()
						.map(s -> "'" + s + "'")
						.collect(Collectors.joining(" or "))));
		}
	}

	/**
	 * Checks if a user-defined function can be easily instantiated.
	 */
	private static void validateInstantiation(Class<?> clazz, boolean requiresDefaultConstructor) {
		if (!InstantiationUtil.isPublic(clazz)) {
			throw new ValidationException(
				String.format(
					"Function class '%s' is not public.",
					clazz.getName()));
		} else if (!InstantiationUtil.isProperClass(clazz)) {
			throw new ValidationException(
				String.format(
					"Function class '%s' is not a proper class. It is either abstract, an interface, or a primitive type.",
					clazz.getName()));
		} else if (requiresDefaultConstructor && !InstantiationUtil.hasPublicNullaryConstructor(clazz)) {
			throw new ValidationException(
				String.format(
					"Function class '%s' must have a public default constructor.",
					clazz.getName()));
		}
	}

	/**
	 * Modifies a function instance by removing any reference to outer classes. This enables
	 * non-static inner function classes.
	 */
	private static void cleanFunction(ReadableConfig config, UserDefinedFunction function) {
		final ClosureCleanerLevel level = config.get(PipelineOptions.CLOSURE_CLEANER_LEVEL);
		ClosureCleaner.clean(function, level, true);
	}

	private UserDefinedFunctionHelper() {
		// no instantiation
	}
}
