package org.apache.flink.table.api;

import org.apache.flink.table.types.DataType;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Watermark metadata defined in {@link TableSchema}. It mainly contains 3 parts:
 *
 * <ol>
 *     <li>the rowtime attribute.</li>
 *     <li>the string representation of watermark generation expression.</li>
 *     <li>the data type of the computation result of watermark generation expression.</li>
 * </ol>
 */
public class WatermarkSpec {

	private final String rowtimeAttribute;

	private final String watermarkExpressionString;

	private final DataType watermarkExprOutputType;

	public WatermarkSpec(String rowtimeAttribute, String watermarkExpressionString, DataType watermarkExprOutputType) {
		this.rowtimeAttribute = checkNotNull(rowtimeAttribute);
		this.watermarkExpressionString = checkNotNull(watermarkExpressionString);
		this.watermarkExprOutputType = checkNotNull(watermarkExprOutputType);
	}

	/**
	 * Returns the name of rowtime attribute, it can be a nested field using dot separator.
	 * The referenced attribute must be present in the {@link TableSchema} and of
	 * type {@link org.apache.flink.table.types.logical.LogicalTypeRoot#TIMESTAMP_WITHOUT_TIME_ZONE}.
	 */
	public String getRowtimeAttribute() {
		return rowtimeAttribute;
	}

	/**
	 * Returns the string representation of watermark generation expression.
	 * The string representation is a qualified SQL expression string (UDFs are expanded).
	 */
	public String getWatermarkExpr() {
		return watermarkExpressionString;
	}

	/**
	 * Returns the data type of the computation result of watermark generation expression.
	 */
	public DataType getWatermarkExprOutputType() {
		return watermarkExprOutputType;
	}


	@Override
	public String toString() {
		return "rowtime: '" + rowtimeAttribute + '\'' +
			", watermark: '" + watermarkExpressionString + '\'';
	}
}
