package org.apache.flink.table.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;

/**
 * Definition of a built-in function. It enables unique identification across different
 * modules by reference equality.
 *
 * <p>Compared to regular {@link FunctionDefinition}, built-in functions have a default name. This
 * default name is used to lookup the function in a catalog during resolution.
 *
 * <p>Equality is defined by reference equality.
 */
@Internal
public final class BuiltInFunctionDefinition implements FunctionDefinition {

	private final String name;
	private final FunctionKind kind;
	private final TypeInference typeInference;

	private BuiltInFunctionDefinition(
			String name,
			FunctionKind kind,
			TypeInference typeInference) {
		this.name = Preconditions.checkNotNull(name, "Name must not be null.");
		this.kind = Preconditions.checkNotNull(kind, "Kind must not be null.");
		this.typeInference = Preconditions.checkNotNull(typeInference, "Type inference must not be null.");
	}

	/**
	 * Builder for configuring and creating instances of {@link BuiltInFunctionDefinition}.
	 */
	public static BuiltInFunctionDefinition.Builder newBuilder() {
		return new BuiltInFunctionDefinition.Builder();
	}

	public String getName() {
		return name;
	}

	@Override
	public FunctionKind getKind() {
		return kind;
	}

	@Override
	public TypeInference getTypeInference(DataTypeFactory typeFactory) {
		return typeInference;
	}

	@Override
	public String toString() {
		return name;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Builder for fluent definition of built-in functions.
	 */
	public static final class Builder {

		private String name;

		private FunctionKind kind;

		private TypeInference.Builder typeInferenceBuilder = TypeInference.newBuilder();

		public Builder() {
			// default constructor to allow a fluent definition
		}

		public Builder name(String name) {
			this.name = name;
			return this;
		}

		public Builder kind(FunctionKind kind) {
			this.kind = kind;
			return this;
		}

		public Builder namedArguments(String... argumentNames) {
			this.typeInferenceBuilder.namedArguments(Arrays.asList(argumentNames));
			return this;
		}

		public Builder typedArguments(DataType... argumentTypes) {
			this.typeInferenceBuilder.typedArguments(Arrays.asList(argumentTypes));
			return this;
		}

		public Builder inputTypeStrategy(InputTypeStrategy inputTypeStrategy) {
			this.typeInferenceBuilder.inputTypeStrategy(inputTypeStrategy);
			return this;
		}

		public Builder accumulatorTypeStrategy(TypeStrategy accumulatorTypeStrategy) {
			this.typeInferenceBuilder.accumulatorTypeStrategy(accumulatorTypeStrategy);
			return this;
		}

		public Builder outputTypeStrategy(TypeStrategy outputTypeStrategy) {
			this.typeInferenceBuilder.outputTypeStrategy(outputTypeStrategy);
			return this;
		}

		public BuiltInFunctionDefinition build() {
			return new BuiltInFunctionDefinition(name, kind, typeInferenceBuilder.build());
		}
	}
}
