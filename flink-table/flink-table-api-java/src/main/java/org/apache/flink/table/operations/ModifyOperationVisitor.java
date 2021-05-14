package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;

/**
 * Class that implements visitor pattern. It allows type safe logic on top of tree
 * of {@link ModifyOperation}s.
 */
@Internal
public interface ModifyOperationVisitor<T> {
	T visit(CatalogSinkModifyOperation catalogSink);

	T visit(OutputConversionModifyOperation outputConversion);

	<U> T visit(UnregisteredSinkModifyOperation<U> unregisteredSink);
}
