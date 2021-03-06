package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.inference.TypeInference;

import java.util.Collections;
import java.util.Set;

/**
 * Definition of a function. Instances of this class provide all details necessary to validate a function
 * call and perform planning.
 *
 * <p>A pure function definition must not contain a runtime implementation. This can be provided by
 * the planner at later stages.
 *
 * @see UserDefinedFunction
 */
@PublicEvolving
public interface FunctionDefinition {

	/**
	 * Returns the kind of function this definition describes.
	 */
	FunctionKind getKind();

	/**
	 * Returns the logic for performing type inference of a call to this function definition.
	 *
	 * <p>The type inference process is responsible for inferring unknown types of input arguments,
	 * validating input arguments, and producing result types. The type inference process happens
	 * independent of a function body. The output of the type inference is used to search for a
	 * corresponding runtime implementation.
	 *
	 * <p>Instances of type inference can be created by using {@link TypeInference#newBuilder()}.
	 *
	 * <p>See {@link BuiltInFunctionDefinitions} for concrete usage examples.
	 */
	TypeInference getTypeInference(DataTypeFactory typeFactory);

	/**
	 * Returns the set of requirements this definition demands.
	 */
	default Set<FunctionRequirement> getRequirements() {
		return Collections.emptySet();
	}

	/**
	 * Returns information about the determinism of the function's results.
	 *
	 * <p>It returns <code>true</code> if and only if a call to this function is guaranteed to
	 * always return the same result given the same parameters. <code>true</code> is
	 * assumed by default. If the function is not pure functional like <code>random(), date(), now(), ...</code>
	 * this method must return <code>false</code>.
	 */
	default boolean isDeterministic() {
		return true;
	}
}
