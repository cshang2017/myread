package org.apache.flink.table.runtime.arrow.readers;

import org.apache.flink.annotation.Internal;

import org.apache.arrow.vector.BitVector;

/**
 * {@link ArrowFieldReader} for Boolean.
 */
@Internal
public final class BooleanFieldReader extends ArrowFieldReader<Boolean> {

	public BooleanFieldReader(BitVector bitVector) {
		super(bitVector);
	}

	@Override
	public Boolean read(int index) {
		return ((BitVector) getValueVector()).getObject(index);
	}
}
