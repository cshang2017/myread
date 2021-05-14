ackage org.apache.flink.table.data.vector;

import org.apache.flink.table.data.ArrayData;

/**
 * Array column vector.
 */
public interface ArrayColumnVector extends ColumnVector {
	ArrayData getArray(int i);
}
