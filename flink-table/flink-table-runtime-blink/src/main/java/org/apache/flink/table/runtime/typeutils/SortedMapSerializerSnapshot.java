package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeutils.NestedSerializersSnapshotDelegate;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.Comparator;
import java.util.SortedMap;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Snapshot class for the {@link SortedMapSerializer}.
 */
public class SortedMapSerializerSnapshot<K, V> implements TypeSerializerSnapshot<SortedMap<K, V>> {

	private Comparator<K> comparator;

	private NestedSerializersSnapshotDelegate nestedSerializersSnapshotDelegate;

	private static final int CURRENT_VERSION = 3;

	@SuppressWarnings("unused")
	public SortedMapSerializerSnapshot() {
		// this constructor is used when restoring from a checkpoint/savepoint.
	}

	SortedMapSerializerSnapshot(SortedMapSerializer<K, V> sortedMapSerializer) {
		this.comparator = sortedMapSerializer.getComparator();
		TypeSerializer[] typeSerializers =
				new TypeSerializer<?>[] { sortedMapSerializer.getKeySerializer(), sortedMapSerializer.getValueSerializer() };
		this.nestedSerializersSnapshotDelegate = new NestedSerializersSnapshotDelegate(typeSerializers);
	}

	@Override
	public int getCurrentVersion() {
		return CURRENT_VERSION;
	}

	@Override
	public void writeSnapshot(DataOutputView out) throws IOException {
		checkState(comparator != null, "Comparator cannot be null.");
		InstantiationUtil.serializeObject(new DataOutputViewStream(out), comparator);
		nestedSerializersSnapshotDelegate.writeNestedSerializerSnapshots(out);
	}

	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		try {
			comparator = InstantiationUtil.deserializeObject(
					new DataInputViewStream(in), userCodeClassLoader);
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
		this.nestedSerializersSnapshotDelegate = NestedSerializersSnapshotDelegate.readNestedSerializerSnapshots(
				in,
				userCodeClassLoader);
	}

	@Override
	public SortedMapSerializer restoreSerializer() {
		TypeSerializer<?>[] nestedSerializers = nestedSerializersSnapshotDelegate.getRestoredNestedSerializers();
		@SuppressWarnings("unchecked")
		TypeSerializer<K> keySerializer = (TypeSerializer<K>) nestedSerializers[0];

		@SuppressWarnings("unchecked")
		TypeSerializer<V> valueSerializer = (TypeSerializer<V>) nestedSerializers[1];

		return new SortedMapSerializer(comparator, keySerializer, valueSerializer);
	}

	@Override
	public TypeSerializerSchemaCompatibility<SortedMap<K, V>> resolveSchemaCompatibility(
			TypeSerializer<SortedMap<K, V>> newSerializer) {
		if (!(newSerializer instanceof SortedMapSerializer)) {
			return TypeSerializerSchemaCompatibility.incompatible();
		}
		SortedMapSerializer newSortedMapSerializer = (SortedMapSerializer) newSerializer;
		if (!comparator.equals(newSortedMapSerializer.getComparator())) {
			return TypeSerializerSchemaCompatibility.incompatible();
		} else {
			return TypeSerializerSchemaCompatibility.compatibleAsIs();
		}
	}
}
