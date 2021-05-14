

package org.apache.flink.api.java.operators.join;

import org.apache.flink.annotation.Public;

/**
 * Join types.
 */
@Public
public enum JoinType {

	INNER, LEFT_OUTER, RIGHT_OUTER, FULL_OUTER;

	public boolean isOuter() {
		return this == LEFT_OUTER || this == RIGHT_OUTER || this == FULL_OUTER;
	}
}
