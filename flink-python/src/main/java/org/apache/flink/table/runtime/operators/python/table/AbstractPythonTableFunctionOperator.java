

package org.apache.flink.table.runtime.operators.python.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.operators.python.AbstractStatelessFunctionOperator;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.core.JoinRelType;

import java.util.ArrayList;
import java.util.List;

/**
 * @param <IN>     Type of the input elements.
 * @param <OUT>    Type of the output elements.
 * @param <UDTFIN> Type of the UDTF input type.
 */
@Internal
public abstract class AbstractPythonTableFunctionOperator<IN, OUT, UDTFIN>
	extends AbstractStatelessFunctionOperator<IN, OUT, UDTFIN> {

	protected final PythonFunctionInfo tableFunction;

	protected final JoinRelType joinType;

	public AbstractPythonTableFunctionOperator(
		Configuration config,
		PythonFunctionInfo tableFunction,
		RowType inputType,
		RowType outputType,
		int[] udtfInputOffsets,
		JoinRelType joinType) {
		super(config, inputType, outputType, udtfInputOffsets);
		this.tableFunction = Preconditions.checkNotNull(tableFunction);
		Preconditions.checkArgument(
			joinType == JoinRelType.INNER || joinType == JoinRelType.LEFT,
			"The join type should be inner join or left join");
		this.joinType = joinType;
	}

	@Override
	public void open() throws Exception {
		List<RowType.RowField> udtfOutputDataFields = new ArrayList<>(
			outputType.getFields().subList(inputType.getFieldCount(), outputType.getFieldCount()));
		userDefinedFunctionOutputType = new RowType(udtfOutputDataFields);
		super.open();
	}

	@Override
	public PythonEnv getPythonEnv() {
		return tableFunction.getPythonFunction().getPythonEnv();
	}

	/**
	 * The received udtf execution result is a finish message when it is a byte with value 0x00.
	 */
	protected boolean isFinishResult(byte[] rawUdtfResult) {
		return rawUdtfResult.length == 1 && rawUdtfResult[0] == 0x00;
	}
}
