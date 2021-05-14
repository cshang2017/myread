package org.apache.flink.table.runtime.functions.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;

/**
 * The abstract base {@link RichFlatMapFunction} used to invoke Python {@link ScalarFunction}
 * functions for the old planner.
 */
@Internal
public abstract class AbstractPythonScalarFunctionFlatMap extends AbstractPythonStatelessFunctionFlatMap {

	/**
	 * The Python {@link ScalarFunction}s to be executed.
	 */
	public final PythonFunctionInfo[] scalarFunctions;

	/**
	 * The offset of the fields which should be forwarded.
	 */
	private final int[] forwardedFields;

	public AbstractPythonScalarFunctionFlatMap(
		Configuration config,
		PythonFunctionInfo[] scalarFunctions,
		RowType inputType,
		RowType outputType,
		int[] udfInputOffsets,
		int[] forwardedFields) {
		super(config, inputType, outputType, udfInputOffsets);
		this.scalarFunctions = Preconditions.checkNotNull(scalarFunctions);
		this.forwardedFields = Preconditions.checkNotNull(forwardedFields);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		RowTypeInfo forwardedInputTypeInfo = new RowTypeInfo(
			Arrays.stream(forwardedFields)
				.mapToObj(i -> inputType.getFields().get(i))
				.map(RowType.RowField::getType)
				.map(TypeConversions::fromLogicalToDataType)
				.map(TypeConversions::fromDataTypeToLegacyInfo)
				.toArray(TypeInformation[]::new));
		forwardedInputSerializer = forwardedInputTypeInfo.createSerializer(getRuntimeContext().getExecutionConfig());
	}

	@Override
	public PythonEnv getPythonEnv() {
		return scalarFunctions[0].getPythonFunction().getPythonEnv();
	}

	@Override
	public void bufferInput(Row input) {
		Row forwardedFieldsRow = Row.project(input, forwardedFields);
		if (getRuntimeContext().getExecutionConfig().isObjectReuseEnabled()) {
			forwardedFieldsRow = forwardedInputSerializer.copy(forwardedFieldsRow);
		}
		forwardedInputQueue.add(forwardedFieldsRow);
	}

	@Override
	public int getForwardedFieldsCount() {
		return forwardedFields.length;
	}
}
