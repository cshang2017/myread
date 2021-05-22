package org.apache.flink.runtime.broadcast;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.runtime.io.network.api.reader.MutableReader;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.runtime.operators.util.ReaderIterator;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class represents a single materialization of a broadcast variable and maintains a reference count for it. If the
 * reference count reaches zero the variable is no longer accessible and will eventually be garbage-collected.
 *
 * @param <T> The type of the elements in the broadcast data set.
 */
public class BroadcastVariableMaterialization<T, C> {

	private final Set<BatchTask<?, ?>> references = new HashSet<BatchTask<?, ?>>();

	private final Object materializationMonitor = new Object();

	private final BroadcastVariableKey key;

	private ArrayList<T> data;

	private C transformed;

	private boolean materialized;

	private boolean disposed;

	public BroadcastVariableMaterialization(BroadcastVariableKey key) {
		this.key = key;
	}

	// --------------------------------------------------------------------------------------------

	public void materializeVariable(MutableReader<?> reader, TypeSerializerFactory<?> serializerFactory, BatchTask<?, ?> referenceHolder)
			throws MaterializationExpiredException, IOException {
		Preconditions.checkNotNull(reader);
		Preconditions.checkNotNull(serializerFactory);
		Preconditions.checkNotNull(referenceHolder);

		final boolean materializer;

		// hold the reference lock only while we track references and decide who should be the materializer
		// that way, other tasks can de-register (in case of failure) while materialization is happening
		synchronized (references) {
			if (disposed) {
				throw new MaterializationExpiredException();
			}

			// sanity check
			if (!references.add(referenceHolder)) {
				throw new IllegalStateException(
						String.format("The task %s already holds a reference to the broadcast variable %s.",
								referenceHolder.getEnvironment().getTaskInfo().getTaskNameWithSubtasks(),
								key.toString()));
			}

			materializer = references.size() == 1;
		}

			MutableReader<DeserializationDelegate<T>> typedReader = (MutableReader<DeserializationDelegate<T>>) reader;

			final TypeSerializer<T> serializer = ((TypeSerializerFactory<T>) serializerFactory).getSerializer();

			final ReaderIterator<T> readerIterator = new ReaderIterator<T>(typedReader, serializer);

			if (materializer) {
				// first one, so we need to materialize;

				ArrayList<T> data = new ArrayList<T>();

				T element;
				while ((element = readerIterator.next()) != null) {
					data.add(element);
				}

				synchronized (materializationMonitor) {
					this.data = data;
					this.materialized = true;
					materializationMonitor.notifyAll();
				}

			}
			else {
				T element = serializer.createInstance();
				while ((element = readerIterator.next(element)) != null) {
				}

				synchronized (materializationMonitor) {
					while (!this.materialized && !disposed) {
						materializationMonitor.wait();
					}
				}

			}
	}

	public boolean decrementReference(BatchTask<?, ?> referenceHolder) {
		return decrementReferenceInternal(referenceHolder, true);
	}

	public boolean decrementReferenceIfHeld(BatchTask<?, ?> referenceHolder) {
		return decrementReferenceInternal(referenceHolder, false);
	}

	private boolean decrementReferenceInternal(BatchTask<?, ?> referenceHolder, boolean errorIfNoReference) {
		synchronized (references) {
			if (disposed || references.isEmpty()) {
				if (errorIfNoReference) {
					throw new IllegalStateException("Decrementing reference to broadcast variable that is no longer alive.");
				} else {
					return false;
				}
			}

			if (!references.remove(referenceHolder)) {
				if (errorIfNoReference) {
					throw new IllegalStateException(
							String.format("The task %s did not hold a reference to the broadcast variable %s.",
									referenceHolder.getEnvironment().getTaskInfo().getTaskNameWithSubtasks(),
									key.toString()));
				} else {
					return false;
				}
			}

			if (references.isEmpty()) {
				disposed = true;
				data = null;
				transformed = null;
				return true;
			} else {
				return false;
			}
		}
	}

	// --------------------------------------------------------------------------------------------

	public List<T> getVariable() throws InitializationTypeConflictException {

		synchronized (references) {
			if (transformed != null) {
				if (transformed instanceof List) {
					@SuppressWarnings("unchecked")
					List<T> casted = (List<T>) transformed;
					return casted;
				}
			}
			else {
				return data;
			}
		}
	}

	public C getVariable(BroadcastVariableInitializer<T, C> initializer) {

		synchronized (references) {
			if (transformed == null) {
				transformed = initializer.initializeBroadcastVariable(data);
				data = null;
			}
			return transformed;
		}
	}
}
