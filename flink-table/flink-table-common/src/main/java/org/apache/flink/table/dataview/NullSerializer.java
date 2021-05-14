package org.apache.flink.table.dataview;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * A serializer for null.
 */
@Internal
public class NullSerializer extends TypeSerializerSingleton<Object> {
	private static final long serialVersionUID = -5381596724707742625L;

	public static final NullSerializer INSTANCE = new NullSerializer();

	private NullSerializer() {}

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public Object createInstance() {
		return null;
	}

	@Override
	public Object copy(Object from) {
		return null;
	}

	@Override
	public Object copy(Object from, Object reuse) {
		return null;
	}

	@Override
	public int getLength() {
		return 0;
	}

	@Override
	public void serialize(Object record, DataOutputView target) throws IOException {
		target.writeByte(0);
	}

	@Override
	public Object deserialize(DataInputView source) throws IOException {
		source.readByte();
		return null;
	}

	@Override
	public Object deserialize(Object reuse, DataInputView source) throws IOException {
		return null;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {

	}

	@Override
	public TypeSerializerSnapshot<Object> snapshotConfiguration() {
		return new NullSerializerSnapshot();
	}

	/**
	 * Serializer configuration snapshot for compatibility and format evolution.
	 */
	@SuppressWarnings("WeakerAccess")
	public static final class NullSerializerSnapshot extends SimpleTypeSerializerSnapshot<Object> {
		public NullSerializerSnapshot() {
			super(() -> NullSerializer.INSTANCE);
		}
	}
}
