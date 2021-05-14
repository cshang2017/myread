package org.apache.flink.api.java.functions;

import org.apache.flink.annotation.Internal;

/**
 * Used as a dummy {@link KeySelector} to allow using keyed operators
 * for non-keyed use cases. Essentially, it gives all incoming records
 * the same key, which is a {@code (byte) 0} value.
 *
 * @param <T> The type of the input element.
 */
@Internal
public class NullByteKeySelector<T> implements KeySelector<T, Byte> {

	@Override
	public Byte getKey(T value) throws Exception {
		return 0;
	}
}
