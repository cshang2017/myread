package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.binary.BinaryRawValueData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;

import java.io.IOException;

/**
 * Serializer for {@link RawValueData}.
 */
@Internal
public final class RawValueDataSerializer<T> extends TypeSerializer<RawValueData<T>> {

	private final TypeSerializer<T> serializer;

	public RawValueDataSerializer(TypeSerializer<T> serializer) {
		this.serializer = serializer;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public RawValueData<T> createInstance() {
		return new BinaryRawValueData<>(serializer.createInstance());
	}

	@Override
	public RawValueData<T> copy(RawValueData<T> from) {
		BinaryRawValueData<T> rawValue = (BinaryRawValueData<T>) from;
		rawValue.ensureMaterialized(serializer);
		byte[] bytes = BinarySegmentUtils.copyToBytes(
			rawValue.getSegments(),
			rawValue.getOffset(),
			rawValue.getSizeInBytes());
		T newJavaObject = rawValue.getJavaObject() == null ? null : serializer.copy(rawValue.getJavaObject());
		return new BinaryRawValueData<>(
			new MemorySegment[]{MemorySegmentFactory.wrap(bytes)},
			0,
			bytes.length,
			newJavaObject);
	}

	@Override
	public RawValueData<T> copy(RawValueData<T> from, RawValueData<T> reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(RawValueData<T> record, DataOutputView target) throws IOException {
		BinaryRawValueData<T> rawValue = (BinaryRawValueData<T>) record;
		rawValue.ensureMaterialized(serializer);
		target.writeInt(rawValue.getSizeInBytes());
		BinarySegmentUtils.copyToView(
			rawValue.getSegments(),
			rawValue.getOffset(),
			rawValue.getSizeInBytes(),
			target);
	}

	@Override
	public RawValueData<T> deserialize(DataInputView source) throws IOException {
		int length = source.readInt();
		byte[] bytes = new byte[length];
		source.readFully(bytes);
		return new BinaryRawValueData<>(
			new MemorySegment[] {MemorySegmentFactory.wrap(bytes)},
			0,
			bytes.length);
	}

	@Override
	public RawValueData<T> deserialize(RawValueData<T> record, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int length = source.readInt();
		target.writeInt(length);
		target.write(source, length);
	}

	@Override
	public RawValueDataSerializer<T> duplicate() {
		return new RawValueDataSerializer<>(serializer.duplicate());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		RawValueDataSerializer that = (RawValueDataSerializer) o;

		return serializer.equals(that.serializer);
	}

	@Override
	public int hashCode() {
		return serializer.hashCode();
	}

	@Override
	public TypeSerializerSnapshot<RawValueData<T>> snapshotConfiguration() {
		return new RawValueDataSerializerSnapshot<>(this);
	}

	public TypeSerializer<T> getInnerSerializer() {
		return serializer;
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link RawValueDataSerializer}.
	 */
	public static final class RawValueDataSerializerSnapshot<T> extends CompositeTypeSerializerSnapshot<RawValueData<T>, RawValueDataSerializer<T>> {

		@SuppressWarnings("unused")
		public RawValueDataSerializerSnapshot() {
			super(RawValueDataSerializer.class);
		}

		public RawValueDataSerializerSnapshot(RawValueDataSerializer<T> serializerInstance) {
			super(serializerInstance);
		}

		@Override
		protected int getCurrentOuterSnapshotVersion() {
			return 0;
		}

		@Override
		protected TypeSerializer<?>[] getNestedSerializers(RawValueDataSerializer<T> outerSerializer) {
			return new TypeSerializer[]{outerSerializer.serializer};
		}

		@Override
		@SuppressWarnings("unchecked")
		protected RawValueDataSerializer<T> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
			return new RawValueDataSerializer<>((TypeSerializer<T>) nestedSerializers[0]);
		}
	}
}
