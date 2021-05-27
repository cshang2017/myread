package org.apache.flink.runtime.state.metainfo;

import org.apache.flink.core.memory.DataInputView;

import javax.annotation.Nonnull;

import java.io.IOException;

/**
 * Functional interface to read {@link StateMetaInfoSnapshot}.
 */
@FunctionalInterface
public interface StateMetaInfoReader {

	/**
	 * Reads a snapshot from the given input view.
	 *
	 * @param inputView the input to read from.
	 * @param userCodeClassLoader user classloader to deserialize the objects in the snapshot.
	 * @return the deserialized snapshot.
	 * @throws IOException on deserialization problems.
	 */
	@Nonnull
	StateMetaInfoSnapshot readStateMetaInfoSnapshot(
		@Nonnull DataInputView inputView,
		@Nonnull ClassLoader userCodeClassLoader) throws IOException;
}
