
package org.apache.flink.runtime;

import org.apache.flink.runtime.jobgraph.OperatorID;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Optional;

/**
 * Formed of a mandatory operator ID and optionally a user defined operator ID.
 */
public class OperatorIDPair implements Serializable {

	private final OperatorID generatedOperatorID;
	private final OperatorID userDefinedOperatorID;

	private OperatorIDPair(OperatorID generatedOperatorID, @Nullable OperatorID userDefinedOperatorID) {
		this.generatedOperatorID = generatedOperatorID;
		this.userDefinedOperatorID = userDefinedOperatorID;
	}

	public static OperatorIDPair of(OperatorID generatedOperatorID, @Nullable OperatorID userDefinedOperatorID) {
		return new OperatorIDPair(generatedOperatorID, userDefinedOperatorID);
	}

	public static OperatorIDPair generatedIDOnly(OperatorID generatedOperatorID) {
		return new OperatorIDPair(generatedOperatorID, null);
	}

	public OperatorID getGeneratedOperatorID() {
		return generatedOperatorID;
	}

	public Optional<OperatorID> getUserDefinedOperatorID() {
		return Optional.ofNullable(userDefinedOperatorID);
	}
}
