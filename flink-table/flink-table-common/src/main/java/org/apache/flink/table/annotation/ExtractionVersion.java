package org.apache.flink.table.annotation;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Logical version that describes the expected behavior of the reflection-based data type extraction.
 *
 * <p>This enumeration is meant for future backward compatibility. Whenever the extraction logic is changed,
 * old function and structured type classes should still return the same data type as before when versioned
 * accordingly.
 */
@PublicEvolving
public enum ExtractionVersion {

	/**
	 * Default if no version is specified.
	 */
	UNKNOWN,

	/**
	 * Initial reflection-based extraction logic according to FLIP-65.
	 */
	V1
}
