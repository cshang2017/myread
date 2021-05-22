
package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.jobgraph.OperatorID;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * An interface to access basic properties of an operator in the context of its coordinator.
 */
public interface OperatorInfo {

	OperatorID operatorId();

	int maxParallelism();

	int currentParallelism();

	// ------------------------------------------------------------------------
	//  utils
	// ------------------------------------------------------------------------

	static Collection<OperatorID> getIds(Collection<? extends OperatorInfo> infos) {
		return infos.stream()
			.map(OperatorInfo::operatorId)
			.collect(Collectors.toList());
	}
}
