package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Identifies a system function with function name or a catalog function with a fully qualified identifier.
 * Function catalog is responsible for resolving an identifier to a function.
 */
@PublicEvolving
public final class FunctionIdentifier implements Serializable {

	private final @Nullable ObjectIdentifier objectIdentifier;

	private final @Nullable String functionName;

	public static FunctionIdentifier of(ObjectIdentifier oi){
		return new FunctionIdentifier(oi);
	}

	public static FunctionIdentifier of(String functionName){
		return new FunctionIdentifier(functionName);
	}

	private FunctionIdentifier(ObjectIdentifier objectIdentifier){
		checkNotNull(objectIdentifier, "Object identifier cannot be null");
		this.objectIdentifier = objectIdentifier;
		this.functionName = null;
	}

	private FunctionIdentifier(String functionName){
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(functionName),
		"function name cannot be null or empty string");
		this.functionName = functionName;
		this.objectIdentifier = null;
	}

	/**
	 * Normalize a function name.
	 */
	public static String normalizeName(String name) {
		return name.toLowerCase();
	}

	/**
	 * Normalize an object identifier by only normalizing the function name.
	 */
	public static ObjectIdentifier normalizeObjectIdentifier(ObjectIdentifier oi) {
		return ObjectIdentifier.of(
			oi.getCatalogName(),
			oi.getDatabaseName(),
			normalizeName(oi.getObjectName()));
	}

	public Optional<ObjectIdentifier> getIdentifier(){
		return Optional.ofNullable(objectIdentifier);
	}

	public Optional<String> getSimpleName(){
		return Optional.ofNullable(functionName);
	}

	/**
	 * List of the component names of this function identifier.
	 */
	public List<String> toList() {
		if (objectIdentifier != null) {
			return objectIdentifier.toList();
		} else if (functionName != null) {
			return Collections.singletonList(functionName);
		} else {
			throw new IllegalStateException(
				"functionName and objectIdentifier are both null which should never happen.");
		}
	}

	/**
	 * Returns a string that summarizes this instance for printing to a console or log.
	 */
	public String asSummaryString() {
		if (objectIdentifier != null) {
			return String.join(".",
				objectIdentifier.getCatalogName(),
				objectIdentifier.getDatabaseName(),
				objectIdentifier.getObjectName());
		} else {
			return functionName;
		}
	}

	@Override
	public String toString() {
		return asSummaryString();
	}
}
