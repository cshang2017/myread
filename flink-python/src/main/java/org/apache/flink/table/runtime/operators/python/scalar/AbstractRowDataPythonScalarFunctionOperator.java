package org.apache.flink.table.runtime.operators.python.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * The Python {@link ScalarFunction} operator for the blink planner.
 */
@Internal
public abstract class AbstractRowDataPythonScalarFunctionOperator
		extends AbstractPythonScalarFunctionOperator<RowData, RowData, RowData> {


	/**
	 * The collector used to collect records.
	 */
	protected transient StreamRecordRowDataWrappingCollector rowDataWrapper;

	/**
	 * The Projection which projects the forwarded fields from the input row.
	 */
	private transient Projection<RowData, BinaryRowData> forwardedFieldProjection;

	/**
	 * The Projection which projects the udf input fields from the input row.
	 */
	private transient Projection<RowData, BinaryRowData> udfInputProjection;

	/**
	 * The JoinedRowData reused holding the execution result.
	 */
	protected transient JoinedRowData reuseJoinedRow;

	public AbstractRowDataPythonScalarFunctionOperator(
		Configuration config,
		PythonFunctionInfo[] scalarFunctions,
		RowType inputType,
		RowType outputType,
		int[] udfInputOffsets,
		int[] forwardedFields) {
		super(config, scalarFunctions, inputType, outputType, udfInputOffsets, forwardedFields);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void open() throws Exception {
		super.open();
		rowDataWrapper = new StreamRecordRowDataWrappingCollector(output);
		reuseJoinedRow = new JoinedRowData();

		udfInputProjection = createUdfInputProjection();
		forwardedFieldProjection = createForwardedFieldProjection();
	}

	@Override
	public void bufferInput(RowData input) {
		// always copy the projection result as the generated Projection reuses the projection result
		RowData forwardedFields = forwardedFieldProjection.apply(input).copy();
		forwardedFields.setRowKind(input.getRowKind());
		forwardedInputQueue.add(forwardedFields);
	}

	@Override
	public RowData getFunctionInput(RowData element) {
		return udfInputProjection.apply(element);
	}

	private Projection<RowData, BinaryRowData> createUdfInputProjection() {
		final GeneratedProjection generatedProjection = ProjectionCodeGenerator.generateProjection(
			CodeGeneratorContext.apply(new TableConfig()),
			"UdfInputProjection",
			inputType,
			userDefinedFunctionInputType,
			userDefinedFunctionInputOffsets);
		// noinspection unchecked
		return generatedProjection.newInstance(Thread.currentThread().getContextClassLoader());
	}

	private Projection<RowData, BinaryRowData> createForwardedFieldProjection() {
		final RowType forwardedFieldType = new RowType(
			Arrays.stream(forwardedFields)
				.mapToObj(i -> inputType.getFields().get(i))
				.collect(Collectors.toList()));
		final GeneratedProjection generatedProjection = ProjectionCodeGenerator.generateProjection(
			CodeGeneratorContext.apply(new TableConfig()),
			"ForwardedFieldProjection",
			inputType,
			forwardedFieldType,
			forwardedFields);
		// noinspection unchecked
		return generatedProjection.newInstance(Thread.currentThread().getContextClassLoader());
	}
}
