package org.apache.flink.table.factories;

import org.apache.flink.annotation.PublicEvolving;

import java.util.List;

/**
 * A factory to create configured table format instances based on string-based properties. See
 * also {@link TableFactory} for more information.
 *
 * @see DeserializationSchemaFactory
 * @see SerializationSchemaFactory
 *
 * @param <T> record type that the format produces or consumes.
 */
@PublicEvolving
public interface TableFormatFactory<T> extends TableFactory {

	/**
	 * Flag to indicate if the given format supports deriving information from a schema. If the
	 * format can handle schema information, those properties must be added to the list of
	 * supported properties.
	 */
	boolean supportsSchemaDerivation();

	/**
	 * List of format property keys that this factory can handle. This method will be used for
	 * validation. If a property is passed that this factory cannot handle, an exception will be
	 * thrown. The list must not contain the keys that are specified by the context.
	 *
	 * <p>Example format properties might be:
	 *   - format.line-delimiter
	 *   - format.ignore-parse-errors
	 *   - format.fields.#.type
	 *   - format.fields.#.name
	 *
	 * <p>If schema derivation is enabled, the list must include schema properties:
	 *   - schema.#.name
	 *   - schema.#.type
	 *
	 * <p>Note: All supported format properties must be prefixed with "format.". If schema derivation is
	 * enabled, also properties with "schema." prefix can be used.
	 *
	 * <p>Use "#" to denote an array of values where "#" represents one or more digits. Property
	 * versions like "format.property-version" must not be part of the supported properties.
	 *
	 * <p>See also {@link TableFactory#supportedProperties()} for more information.
	 */
	List<String> supportedProperties();
}
