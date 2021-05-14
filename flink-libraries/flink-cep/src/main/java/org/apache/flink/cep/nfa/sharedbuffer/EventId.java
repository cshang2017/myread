
package org.apache.flink.cep.nfa.sharedbuffer;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Comparator;
import java.util.Objects;

/**
 * Composite key for events in {@link SharedBuffer}.
 */
public class EventId implements Comparable<EventId> {
	private final int id;
	private final long timestamp;

	public EventId(int id, long timestamp) {
		this.id = id;
		this.timestamp = timestamp;
	}

	public int getId() {
		return id;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public static final Comparator<EventId> COMPARATOR = Comparator.comparingLong(EventId::getTimestamp)
		.thenComparingInt(EventId::getId);

	@Override
	public int compareTo(EventId o) {
		return COMPARATOR.compare(this, o);
	}

	/** {@link TypeSerializer} for {@link EventId}. */
	public static class EventIdSerializer extends TypeSerializerSingleton<EventId> {

		private EventIdSerializer() {
		}

		public static final EventIdSerializer INSTANCE = new EventIdSerializer();

		@Override
		public boolean isImmutableType() {
			return true;
		}

		@Override
		public EventId createInstance() {
			return null;
		}

		@Override
		public EventId copy(EventId from) {
			return new EventId(from.id, from.timestamp);
		}

		@Override
		public EventId copy(EventId from, EventId reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			return Integer.BYTES + Long.BYTES;
		}

		@Override
		public void serialize(EventId record, DataOutputView target) throws IOException {
			target.writeInt(record.id);
			target.writeLong(record.timestamp);
		}

		@Override
		public EventId deserialize(DataInputView source) throws IOException {
			int id = source.readInt();
			long timestamp = source.readLong();

			return new EventId(id, timestamp);
		}

		@Override
		public EventId deserialize(EventId reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			target.writeInt(source.readInt());
			target.writeLong(source.readLong());
		}

		// -----------------------------------------------------------------------------------

		@Override
		public TypeSerializerSnapshot<EventId> snapshotConfiguration() {
			return new EventIdSerializerSnapshot();
		}

		/**
		 * Serializer configuration snapshot for compatibility and format evolution.
		 */
		@SuppressWarnings("WeakerAccess")
		public static final class EventIdSerializerSnapshot extends SimpleTypeSerializerSnapshot<EventId> {

			public EventIdSerializerSnapshot() {
				super(() -> INSTANCE);
			}
		}
	}
}
