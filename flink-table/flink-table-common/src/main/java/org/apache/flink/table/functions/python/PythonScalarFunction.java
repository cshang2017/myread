package org.apache.flink.table.functions.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The wrapper of user defined python scalar function.
 */
@Internal
public class PythonScalarFunction extends ScalarFunction implements PythonFunction {

	private final String name;
	private final byte[] serializedScalarFunction;
	private final TypeInformation[] inputTypes;
	private final TypeInformation resultType;
	private final PythonFunctionKind pythonFunctionKind;
	private final boolean deterministic;
	private final PythonEnv pythonEnv;

	public PythonScalarFunction(
		String name,
		byte[] serializedScalarFunction,
		TypeInformation[] inputTypes,
		TypeInformation resultType,
		PythonFunctionKind pythonFunctionKind,
		boolean deterministic,
		PythonEnv pythonEnv) {
		this.name = name;
		this.serializedScalarFunction = serializedScalarFunction;
		this.inputTypes = inputTypes;
		this.resultType = resultType;
		this.pythonFunctionKind = pythonFunctionKind;
		this.deterministic = deterministic;
		this.pythonEnv = pythonEnv;
	}

	public Object eval(Object... args) {
		throw new UnsupportedOperationException(
			"This method is a placeholder and should not be called.");
	}

	@Override
	public byte[] getSerializedPythonFunction() {
		return serializedScalarFunction;
	}

	@Override
	public PythonEnv getPythonEnv() {
		return pythonEnv;
	}

	@Override
	public PythonFunctionKind getPythonFunctionKind() {
		return pythonFunctionKind;
	}

	@Override
	public boolean isDeterministic() {
		return deterministic;
	}

	@Override
	public TypeInformation[] getParameterTypes(Class[] signature) {
		return inputTypes;
	}

	@Override
	public TypeInformation getResultType(Class[] signature) {
		return resultType;
	}

	@Override
	public TypeInference getTypeInference(DataTypeFactory typeFactory) {
		final List<DataType> argumentDataTypes = Stream.of(inputTypes)
			.map(TypeConversions::fromLegacyInfoToDataType)
			.collect(Collectors.toList());
		return TypeInference.newBuilder()
			.typedArguments(argumentDataTypes)
			.outputTypeStrategy(TypeStrategies.explicit(TypeConversions.fromLegacyInfoToDataType(resultType)))
			.build();
	}

	@Override
	public String toString() {
		return name;
	}
}
