package org.apache.flink.api.common.python.pickle;

import org.apache.commons.lang3.ArrayUtils;

/**
 * Creates byte arrays (byte[]). Deal with an empty byte array pickled by Python 3.
 */
public final class ByteArrayConstructor extends net.razorvine.pickle.objects.ByteArrayConstructor {

	@Override
	public Object construct(Object[] args) {
		if (args.length == 0) {
			return ArrayUtils.EMPTY_BYTE_ARRAY;
		} else {
			return super.construct(args);
		}
	}
}
