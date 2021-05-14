package org.apache.flink.table.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.SqlTimestampComparator;
import org.apache.flink.api.common.typeutils.base.SqlTimestampSerializer;
import org.apache.flink.table.api.DataTypes;

import java.sql.Timestamp;

/**
 * Type information for indicating event or processing time. However, it behaves like a
 * regular SQL timestamp but is serialized as Long.
 *
 * @deprecated This class will be removed in future versions as it is used for the old type system. It
 *             is recommended to use {@link DataTypes} instead. Please make sure to use either the old
 *             or the new type system consistently to avoid unintended behavior. See the website documentation
 *             for more information.
 */
@Internal
@Deprecated
public class TimeIndicatorTypeInfo extends SqlTimeTypeInfo<Timestamp> {

	private final boolean isEventTime;

	public static final int ROWTIME_STREAM_MARKER = -1;
	public static final int PROCTIME_STREAM_MARKER = -2;

	public static final int ROWTIME_BATCH_MARKER = -3;
	public static final int PROCTIME_BATCH_MARKER = -4;

	public static final TimeIndicatorTypeInfo ROWTIME_INDICATOR = new TimeIndicatorTypeInfo(true);
	public static final TimeIndicatorTypeInfo PROCTIME_INDICATOR = new TimeIndicatorTypeInfo(false);

	@SuppressWarnings("unchecked")
	protected TimeIndicatorTypeInfo(boolean isEventTime) {
		super(Timestamp.class, SqlTimestampSerializer.INSTANCE, (Class) SqlTimestampComparator.class);
		this.isEventTime = isEventTime;
	}

	// this replaces the effective serializer by a LongSerializer
	// it is a hacky but efficient solution to keep the object creation overhead low but still
	// be compatible with the corresponding SqlTimestampTypeInfo
	@Override
	@SuppressWarnings("unchecked")
	public TypeSerializer<Timestamp> createSerializer(ExecutionConfig executionConfig) {
		return (TypeSerializer) LongSerializer.INSTANCE;
	}

	public boolean isEventTime() {
		return isEventTime;
	}

	@Override
	public String toString() {
		if (isEventTime) {
			return "TimeIndicatorTypeInfo(rowtime)";
		} else {
			return "TimeIndicatorTypeInfo(proctime)";
		}
	}
}

