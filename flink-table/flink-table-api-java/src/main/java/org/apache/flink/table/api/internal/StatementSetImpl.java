package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implementation for {@link StatementSet}.
 */
@Internal
class StatementSetImpl implements StatementSet {
	private final TableEnvironmentInternal tableEnvironment;
	private List<ModifyOperation> operations = new ArrayList<>();

	protected StatementSetImpl(TableEnvironmentInternal tableEnvironment) {
		this.tableEnvironment = tableEnvironment;
	}

	@Override
	public StatementSet addInsertSql(String statement) {
		List<Operation> operations = tableEnvironment.getParser().parse(statement);


		Operation operation = operations.get(0);
		if (operation instanceof ModifyOperation) {
			this.operations.add((ModifyOperation) operation);
		} else {
			throw new TableException("Only insert statement is supported now.");
		}
		return this;
	}

	@Override
	public StatementSet addInsert(String targetPath, Table table) {
		return addInsert(targetPath, table, false);
	}

	@Override
	public StatementSet addInsert(String targetPath, Table table, boolean overwrite) {
		UnresolvedIdentifier unresolvedIdentifier = tableEnvironment.getParser().parseIdentifier(targetPath);
		ObjectIdentifier objectIdentifier = tableEnvironment.getCatalogManager()
				.qualifyIdentifier(unresolvedIdentifier);

		operations.add(new CatalogSinkModifyOperation(
				objectIdentifier,
				table.getQueryOperation(), // child QueryOperation
				Collections.emptyMap(), // staticPartition
				overwrite,
				Collections.emptyMap())); // dynamicOptions

		return this;
	}

	@Override
	public String explain(ExplainDetail... extraDetails) {
		List<Operation> operationList = operations.stream().map(o -> (Operation) o).collect(Collectors.toList());
		return tableEnvironment.explainInternal(operationList, extraDetails);
	}

	@Override
	public TableResult execute() {
		try {
			return tableEnvironment.executeInternal(operations);
		} finally {
			operations.clear();
		}
	}
}
