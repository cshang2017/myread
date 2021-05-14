
package org.apache.flink.table.runtime.arrow.readers;

import org.apache.flink.annotation.Internal;

import org.apache.arrow.vector.BigIntVector;

/**
 * {@link ArrowFieldReader} for BigInt.
 */
@Internal
public final class BigIntFieldReader extends ArrowFieldReader<Long> {

	public BigIntFieldReader(BigIntVector bigIntVector) {
		super(bigIntVector);
	}

	@Override
	public Long read(int index) {
		return ((BigIntVector) getValueVector()).getObject(index);
	}
}
