package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Map;

/**
 * Interface that adds a set of string-based, normalized properties for describing DDL information.
 *
 * <p>Typical characteristics of a descriptor are:
 * - descriptors have a default constructor
 * - descriptors themselves contain very little logic
 * - corresponding validators validate the correctness (goal: have a single point of validation)
 *
 * <p>A descriptor is similar to a builder in a builder pattern, thus, mutable for building
 * properties.
 */
@PublicEvolving
public interface Descriptor {

	/**
	 * Converts this descriptor into a set of properties.
	 */
	Map<String, String> toProperties();
}
