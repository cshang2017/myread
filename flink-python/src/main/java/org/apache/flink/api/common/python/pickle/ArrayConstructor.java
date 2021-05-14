package org.apache.flink.api.common.python.pickle;

import java.util.ArrayList;

/**
 * Creates arrays of objects. Returns a primitive type array such as int[] if
 * the objects are ints, etc. Returns an ArrayList if it needs to
 * contain arbitrary objects (such as lists).
 */
public final class ArrayConstructor extends net.razorvine.pickle.objects.ArrayConstructor {

	@Override
	public Object construct(Object[] args) {
		if (args.length == 2 && args[0] == "l") {
			// an array of typecode 'l' should be handled as long rather than int.
			ArrayList<Object> values = (ArrayList<Object>) args[1];
			long[] result = new long[values.size()];
			int i = 0;
			while (i < values.size()) {
				result[i] = ((Number) values.get(i)).longValue();
				i += 1;
			}
			return result;
		}

		return super.construct(args);
	}
}
