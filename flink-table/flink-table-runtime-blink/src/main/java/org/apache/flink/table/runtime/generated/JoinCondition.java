
package org.apache.flink.table.runtime.generated;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.table.data.RowData;

/**
 * Interface for code generated condition function for [[org.apache.calcite.rel.core.Join]].
 */
public interface JoinCondition extends RichFunction {

	/**
	 * @return true if the join condition stays true for the joined row (in1, in2)
	 */
	boolean apply(RowData in1, RowData in2);

}
