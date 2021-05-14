

package org.apache.flink.table.runtime.operators.match;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

/**
 * ProcessFunction to copy a timestamp from a {@link RowData} field into the
 * {@link org.apache.flink.streaming.runtime.streamrecord.StreamRecord}.
 */
public class RowtimeProcessFunction
	extends ProcessFunction<RowData, RowData> implements ResultTypeQueryable<RowData> {

	private final int rowtimeIdx;
	private final int precision;
	private transient TypeInformation<RowData> returnType;

	public RowtimeProcessFunction(int rowtimeIdx, TypeInformation<RowData> returnType, int precision) {
		this.rowtimeIdx = rowtimeIdx;
		this.returnType = returnType;
		this.precision = precision;
	}

	@Override
	public void processElement(RowData value, Context ctx, Collector<RowData> out) throws Exception {
		long timestamp = value.getTimestamp(rowtimeIdx, precision).getMillisecond();
		((TimestampedCollector<RowData>) out).setAbsoluteTimestamp(timestamp);
		out.collect(value);
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return returnType;
	}
}
