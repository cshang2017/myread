package org.apache.flink.table.runtime.arrow.readers;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.complex.ListVector;

import java.lang.reflect.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;


/**
 * {@link ArrowFieldReader} for Array.
 */
@Internal
public final class ArrayFieldReader extends ArrowFieldReader<Object[]> {

	private final ArrowFieldReader arrayData;
	private final Class<?> elementClass;

	public ArrayFieldReader(ListVector listVector, ArrowFieldReader arrayData, LogicalType elementType) {
		super(listVector);
		this.arrayData = Preconditions.checkNotNull(arrayData);
		this.elementClass = getElementClass(elementType);
	}

	@Override
	public Object[] read(int index) {
		if (getValueVector().isNull(index)) {
			return null;
		} else {
			int startIndex = index * ListVector.OFFSET_WIDTH;
			int start = getValueVector().getOffsetBuffer().getInt(startIndex);
			int end = getValueVector().getOffsetBuffer().getInt(startIndex + ListVector.OFFSET_WIDTH);
			Object[] result = (Object[]) Array.newInstance(elementClass, end - start);
			for (int i = 0; i < result.length; i++) {
				result[i] = arrayData.read(start + i);
			}
			return result;
		}
	}

	private Class<?> getElementClass(LogicalType elementType) {
		DataType dataType = TypeConversions.fromLogicalToDataType(elementType);
		if (elementType instanceof TimestampType) {
			// the default conversion class is java.time.LocalDateTime
			dataType = dataType.bridgedTo(Timestamp.class);
		} else if (elementType instanceof DateType) {
			// the default conversion class is java.time.LocalDate
			dataType = dataType.bridgedTo(Date.class);
		} else if (elementType instanceof TimeType) {
			// the default conversion class is java.time.LocalTime
			dataType = dataType.bridgedTo(Time.class);
		}
		return dataType.getConversionClass();
	}
}
