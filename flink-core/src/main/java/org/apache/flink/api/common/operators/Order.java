package org.apache.flink.api.common.operators;

import org.apache.flink.annotation.Public;

/**
 * Enumeration representing order. May represent no order, an ascending order or a descending order.
 */
@Public
public enum Order {
	
	/**
	 * Indicates no order.
	 */
	NONE,

	/**
	 * Indicates an ascending order.
	 */
	ASCENDING,

	/**
	 * Indicates a descending order.
	 */
	DESCENDING,

	/**
	 * Indicates an order without a direction. This constant is not used to indicate
	 * any existing order, but for example to indicate that an order of any direction
	 * is desirable.
	 */
	ANY;
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Checks, if this enum constant represents in fact an order. That is,
	 * whether this property is not equal to <tt>Order.NONE</tt>.
	 * 
	 * @return True, if this enum constant is unequal to <tt>Order.NONE</tt>,
	 *         false otherwise.
	 */
	public boolean isOrdered() {
		return this != Order.NONE;
	}
	
	public String getShortName() {
		return this == ASCENDING ? "ASC" : this == DESCENDING ? "DESC" : this == ANY ? "*" : "-";
	}
}
