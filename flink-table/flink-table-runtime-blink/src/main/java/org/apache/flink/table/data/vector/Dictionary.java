package org.apache.flink.table.data.vector;

import org.apache.flink.table.data.TimestampData;

/**
 * The interface for dictionary in AbstractColumnVector to decode dictionary encoded values.
 */
public interface Dictionary {

	int decodeToInt(int id);

	long decodeToLong(int id);

	float decodeToFloat(int id);

	double decodeToDouble(int id);

	byte[] decodeToBinary(int id);

	TimestampData decodeToTimestamp(int id);
}
