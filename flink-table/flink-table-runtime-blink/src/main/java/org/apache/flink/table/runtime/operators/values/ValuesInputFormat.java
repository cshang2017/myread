package org.apache.flink.table.runtime.operators.values;

import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedInput;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Generated ValuesInputFormat.
 */
public class ValuesInputFormat
		extends GenericInputFormat<RowData>
		implements NonParallelInput, ResultTypeQueryable<RowData> {

	private static final Logger LOG = LoggerFactory.getLogger(ValuesInputFormat.class);
	private static final long serialVersionUID = 1L;

	private GeneratedInput<GenericInputFormat<RowData>> generatedInput;
	private final RowDataTypeInfo returnType;
	private GenericInputFormat<RowData> format;

	public ValuesInputFormat(GeneratedInput<GenericInputFormat<RowData>> generatedInput, RowDataTypeInfo returnType) {
		this.generatedInput = generatedInput;
		this.returnType = returnType;
	}

	@Override
	public void open(GenericInputSplit split) {
		LOG.debug("Compiling GenericInputFormat: {} \n\n Code:\n{}",
				generatedInput.getClassName(), generatedInput.getCode());
		LOG.debug("Instantiating GenericInputFormat.");

		format = generatedInput.newInstance(getRuntimeContext().getUserCodeClassLoader());
		generatedInput = null;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return format.reachedEnd();
	}

	@Override
	public RowData nextRecord(RowData reuse) throws IOException {
		return format.nextRecord(reuse);
	}

	@Override
	public RowDataTypeInfo getProducedType() {
		return returnType;
	}

}
