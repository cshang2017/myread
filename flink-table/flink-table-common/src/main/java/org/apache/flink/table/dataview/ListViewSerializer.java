package org.apache.flink.table.dataview;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.LegacySerializerSnapshotTransformer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.CollectionSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.base.ListSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.api.dataview.ListView;

import java.io.IOException;
import java.util.List;

/**
 * A serializer for [[ListView]]. The serializer relies on an element
 * serializer for the serialization of the list's elements.
 *
 * <p>The serialization format for the list is as follows: four bytes for the length of the list,
 * followed by the serialized representation of each element.
 *
 * @param <T> The type of element in the list.
 */
@Internal
public class ListViewSerializer<T>
		extends TypeSerializer<ListView<T>>
		implements LegacySerializerSnapshotTransformer<ListView<T>> {

	private static final long serialVersionUID = -2030398712359267867L;

	private final TypeSerializer<List<T>> listSerializer;

	public ListViewSerializer(TypeSerializer<List<T>> listSerializer) {
		this.listSerializer = listSerializer;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<ListView<T>> duplicate() {
		return new ListViewSerializer<>(listSerializer.duplicate());
	}

	@Override
	public ListView<T> createInstance() {
		return new ListView<>();
	}

	@Override
	public ListView<T> copy(ListView<T> from) {
		return new ListView<>(null, listSerializer.copy(from.list));
	}

	@Override
	public ListView<T> copy(ListView<T> from, ListView<T> reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(ListView<T> record, DataOutputView target) throws IOException {
		listSerializer.serialize(record.list, target);
	}

	@Override
	public ListView<T> deserialize(DataInputView source) throws IOException {
		return new ListView<>(null, listSerializer.deserialize(source));
	}

	@Override
	public ListView<T> deserialize(ListView<T> reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		listSerializer.copy(source, target);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ListViewSerializer) {
			ListViewSerializer<?> other = (ListViewSerializer<?>) obj;
			return listSerializer.equals(other.listSerializer);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return listSerializer.hashCode();
	}

	@Override
	public TypeSerializerSnapshot<ListView<T>> snapshotConfiguration() {
		return new ListViewSerializerSnapshot<>(this);
	}

	/**
	 * We need to override this as a {@link LegacySerializerSnapshotTransformer}
	 * because in Flink 1.6.x and below, this serializer was incorrectly returning
	 * directly the snapshot of the nested list serializer as its own snapshot.
	 *
	 * <p>This method transforms the incorrect list serializer snapshot
	 * to be a proper {@link ListViewSerializerSnapshot}.
	 */
	@Override
	public <U> TypeSerializerSnapshot<ListView<T>> transformLegacySerializerSnapshot(
			TypeSerializerSnapshot<U> legacySnapshot) {
		if (legacySnapshot instanceof ListViewSerializerSnapshot) {
			return (TypeSerializerSnapshot<ListView<T>>) legacySnapshot;
		} else if (legacySnapshot instanceof CollectionSerializerConfigSnapshot) {
			// first, transform the incorrect list serializer's snapshot
			// into a proper ListSerializerSnapshot
			ListSerializerSnapshot<T> transformedNestedListSerializerSnapshot = new ListSerializerSnapshot<>();
			CollectionSerializerConfigSnapshot<List<T>, T> snapshot =
					(CollectionSerializerConfigSnapshot<List<T>, T>) legacySnapshot;
			CompositeTypeSerializerUtil.setNestedSerializersSnapshots(
					transformedNestedListSerializerSnapshot,
					(TypeSerializerSnapshot<?>) (snapshot.getSingleNestedSerializerAndConfig().f1));

			// then, wrap the transformed ListSerializerSnapshot
			// as a nested snapshot in the final resulting ListViewSerializerSnapshot
			ListViewSerializerSnapshot<T> transformedListViewSerializerSnapshot = new ListViewSerializerSnapshot<>();
			CompositeTypeSerializerUtil.setNestedSerializersSnapshots(
					transformedListViewSerializerSnapshot,
					transformedNestedListSerializerSnapshot);

			return transformedListViewSerializerSnapshot;
		} else {
			throw new UnsupportedOperationException(
					legacySnapshot.getClass().getCanonicalName() + " is not supported.");
		}
	}

	public TypeSerializer<List<T>> getListSerializer() {
		return listSerializer;
	}

}
