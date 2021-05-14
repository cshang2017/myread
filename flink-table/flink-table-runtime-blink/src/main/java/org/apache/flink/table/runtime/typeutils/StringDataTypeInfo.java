package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.StringData;

/**
 * TypeInformation for {@link StringData}.
 */
@Internal
public class StringDataTypeInfo extends TypeInformation<StringData> {

	public static final StringDataTypeInfo INSTANCE = new StringDataTypeInfo();

	private StringDataTypeInfo() {}

	@Override
	public boolean isBasicType() {
		return true;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 1;
	}

	@Override
	public int getTotalFields() {
		return 1;
	}

	@Override
	public Class<StringData> getTypeClass() {
		return StringData.class;
	}

	@Override
	public boolean isKeyType() {
		return true;
	}

	@Override
	public TypeSerializer<StringData> createSerializer(ExecutionConfig config) {
		return StringDataSerializer.INSTANCE;
	}

	@Override
	public String toString() {
		return "StringData";
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof StringDataTypeInfo;
	}

	@Override
	public int hashCode() {
		return 0;
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof StringDataTypeInfo;
	}
}
