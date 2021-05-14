

package org.apache.flink.api.java.aggregation;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Exception indicating an unsupported type was used for an aggregation.
 */
@PublicEvolving
public class UnsupportedAggregationTypeException extends RuntimeException {


	public UnsupportedAggregationTypeException(String message) {
		super(message);
	}

}
