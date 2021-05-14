

package org.apache.flink.table.planner.functions.tablefunctions;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Replicate the row N times. N is specified as the first argument to the function.
 * This is an internal function solely used by optimizer to rewrite EXCEPT ALL AND
 * INTERSECT ALL queries.
 */
public class ReplicateRows extends TableFunction<Row> {

	private final TypeInformation[] fieldTypes;
	private transient Row reuseRow;

	public ReplicateRows(TypeInformation[] fieldTypes) {
		this.fieldTypes = fieldTypes;
	}

	public void eval(Object... inputs) {
		checkArgument(inputs.length == fieldTypes.length + 1);
		long numRows = (long) inputs[0];
		if (reuseRow == null) {
			reuseRow = new Row(fieldTypes.length);
		}
		for (int i = 0; i < fieldTypes.length; i++) {
			reuseRow.setField(i, inputs[i + 1]);
		}
		for (int i = 0; i < numRows; i++) {
			collect(reuseRow);
		}
	}

	@Override
	public TypeInformation<Row> getResultType() {
		return new RowTypeInfo(fieldTypes);
	}

	@Override
	public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
		TypeInformation[] paraTypes = new TypeInformation[1 + fieldTypes.length];
		paraTypes[0] = Types.LONG;
		System.arraycopy(fieldTypes, 0, paraTypes, 1, fieldTypes.length);
		return paraTypes;
	}
}
