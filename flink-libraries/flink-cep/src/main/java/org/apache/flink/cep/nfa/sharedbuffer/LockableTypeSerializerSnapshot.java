package org.apache.flink.cep.nfa.sharedbuffer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;

/**
 * A {@link TypeSerializerSnapshot} for the {@link Lockable.LockableTypeSerializer}.
 */
@Internal
public class LockableTypeSerializerSnapshot<E> extends CompositeTypeSerializerSnapshot<Lockable<E>, Lockable.LockableTypeSerializer<E>> {

	private static final int CURRENT_VERSION = 1;

	/**
	 * Constructor for read instantiation.
	 */
	public LockableTypeSerializerSnapshot() {
		super(Lockable.LockableTypeSerializer.class);
	}

	/**
	 * Constructor to create the snapshot for writing.
	 */
	public LockableTypeSerializerSnapshot(Lockable.LockableTypeSerializer<E> lockableTypeSerializer) {
		super(lockableTypeSerializer);
	}

	@Override
	public int getCurrentOuterSnapshotVersion() {
		return CURRENT_VERSION;
	}

	@Override
	protected Lockable.LockableTypeSerializer<E> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
		@SuppressWarnings("unchecked")
		TypeSerializer<E> elementSerializer = (TypeSerializer<E>) nestedSerializers[0];
		return new Lockable.LockableTypeSerializer<>(elementSerializer);
	}

	@Override
	protected TypeSerializer<?>[] getNestedSerializers(Lockable.LockableTypeSerializer<E> outerSerializer) {
		return new TypeSerializer<?>[] { outerSerializer.getElementSerializer() };
	}
}
