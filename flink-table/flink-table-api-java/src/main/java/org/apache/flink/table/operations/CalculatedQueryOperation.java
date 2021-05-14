
package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.TableFunction;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Describes a relational operation that was created from applying a {@link TableFunction}.
 */
@Internal
public class CalculatedQueryOperation implements QueryOperation {

	private final FunctionDefinition functionDefinition;
	private final @Nullable FunctionIdentifier functionIdentifier;
	private final List<ResolvedExpression> arguments;
	private final TableSchema tableSchema;

	public CalculatedQueryOperation(
			FunctionDefinition functionDefinition,
			@Nullable FunctionIdentifier functionIdentifier,
			List<ResolvedExpression> arguments,
			TableSchema tableSchema) {
		this.functionDefinition = functionDefinition;
		this.functionIdentifier = functionIdentifier;
		this.arguments = arguments;
		this.tableSchema = tableSchema;
	}

	public FunctionDefinition getFunctionDefinition() {
		return functionDefinition;
	}

	public Optional<FunctionIdentifier> getFunctionIdentifier() {
		return Optional.ofNullable(functionIdentifier);
	}

	public List<ResolvedExpression> getArguments() {
		return arguments;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> args = new LinkedHashMap<>();
		if (functionIdentifier != null) {
			args.put("function", functionIdentifier);
		} else {
			args.put("function", functionDefinition.toString());
		}
		args.put("arguments", arguments);

		return OperationUtils.formatWithChildren("CalculatedTable", args, getChildren(), Operation::asSummaryString);
	}

	@Override
	public List<QueryOperation> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <U> U accept(QueryOperationVisitor<U> visitor) {
		return visitor.visit(this);
	}
}
