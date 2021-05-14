
package org.apache.flink.table.dataview;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.table.api.dataview.ListView;

import java.util.List;

/**
 * A {@link TypeSerializerSnapshot} for the {@link ListViewSerializer}.
 *
 * @param <T> the type of the list elements.
 */
@Internal
public final class ListViewSerializerSnapshot<T> extends CompositeTypeSerializerSnapshot<ListView<T>, ListViewSerializer<T>> {

	private static final int CURRENT_VERSION = 1;

	/**
	 * Constructor for read instantiation.
	 */
	public ListViewSerializerSnapshot() {
		super(ListViewSerializer.class);
	}

	/**
	 * Constructor to create the snapshot for writing.
	 */
	public ListViewSerializerSnapshot(ListViewSerializer<T> listViewSerializer) {
		super(listViewSerializer);
	}

	@Override
	public int getCurrentOuterSnapshotVersion() {
		return CURRENT_VERSION;
	}

	@Override
	protected ListViewSerializer<T> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
		@SuppressWarnings("unchecked")
		TypeSerializer<List<T>> listSerializer = (TypeSerializer<List<T>>) nestedSerializers[0];
		return new ListViewSerializer<>(listSerializer);
	}

	@Override
	protected TypeSerializer<?>[] getNestedSerializers(ListViewSerializer<T> outerSerializer) {
		return new TypeSerializer<?>[] { outerSerializer.getListSerializer() };
	}
}
