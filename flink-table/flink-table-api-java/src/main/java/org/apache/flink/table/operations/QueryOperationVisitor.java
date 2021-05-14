package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;

// READ

/**
 * Class that implements visitor pattern. It allows type safe logic on top of tree
 * of {@link QueryOperation}s.
 */
@Internal
public interface QueryOperationVisitor<T> {

	T visit(ProjectQueryOperation projection);

	T visit(AggregateQueryOperation aggregation);

	T visit(WindowAggregateQueryOperation windowAggregate);

	T visit(JoinQueryOperation join);

	T visit(SetQueryOperation setOperation);

	T visit(FilterQueryOperation filter);

	T visit(DistinctQueryOperation distinct);

	T visit(SortQueryOperation sort);

	T visit(CalculatedQueryOperation calculatedTable);

	T visit(CatalogQueryOperation catalogTable);

	T visit(ValuesQueryOperation values);

	<U> T visit(TableSourceQueryOperation<U> tableSourceTable);

	T visit(QueryOperation other);
}
