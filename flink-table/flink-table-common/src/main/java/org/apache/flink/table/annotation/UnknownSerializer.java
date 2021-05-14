package org.apache.flink.table.annotation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * Helper class for {@link DataTypeHint} for representing an unknown serializer that should be
 * replaced with a more specific class.
 */
@Internal
abstract class UnknownSerializer extends TypeSerializer<Object> {

	private UnknownSerializer() {
		// no instantiation
	}
}
