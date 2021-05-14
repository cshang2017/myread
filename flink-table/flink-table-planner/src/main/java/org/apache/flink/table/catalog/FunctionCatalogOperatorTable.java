package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * Thin adapter between {@link SqlOperatorTable} and {@link FunctionCatalog}.
 */
@Internal
public class FunctionCatalogOperatorTable implements SqlOperatorTable {

	private final FunctionCatalog functionCatalog;
	private final FlinkTypeFactory typeFactory;

	public FunctionCatalogOperatorTable(
			FunctionCatalog functionCatalog,
			FlinkTypeFactory typeFactory) {
		this.functionCatalog = functionCatalog;
		this.typeFactory = typeFactory;
	}

	@Override
	public void lookupOperatorOverloads(
			SqlIdentifier opName,
			SqlFunctionCategory category,
			SqlSyntax syntax,
			List<SqlOperator> operatorList,
			SqlNameMatcher nameMatcher) {
		if (!opName.isSimple()) {
			return;
		}

		// We lookup only user functions via CatalogOperatorTable. Built in functions should
		// go through BasicOperatorTable
		if (isNotUserFunction(category)) {
			return;
		}

		String name = opName.getSimple();
		Optional<FunctionLookup.Result> candidateFunction = functionCatalog.lookupFunction(
			UnresolvedIdentifier.of(name));

		candidateFunction.flatMap(lookupResult ->
			convertToSqlFunction(category, name, lookupResult.getFunctionDefinition())
		).ifPresent(operatorList::add);
	}

	private boolean isNotUserFunction(SqlFunctionCategory category) {
		return category != null && !category.isUserDefinedNotSpecificFunction();
	}

	private Optional<SqlFunction> convertToSqlFunction(
			SqlFunctionCategory category,
			String name,
			FunctionDefinition functionDefinition) {
		if (functionDefinition instanceof AggregateFunctionDefinition) {
			return convertAggregateFunction(name, (AggregateFunctionDefinition) functionDefinition);
		} else if (functionDefinition instanceof ScalarFunctionDefinition) {
			return convertScalarFunction(name, (ScalarFunctionDefinition) functionDefinition);
		} else if (functionDefinition instanceof TableFunctionDefinition &&
				category != null &&
				category.isTableFunction()) {
			return convertTableFunction(name, (TableFunctionDefinition) functionDefinition);
		} else if (functionDefinition instanceof BuiltInFunctionDefinition) {
			return Optional.empty();
		}
	
		if (functionDefinition instanceof ScalarFunction) {
			return convertToSqlFunction(
				category,
				name,
				new ScalarFunctionDefinition(
					name,
					(ScalarFunction) functionDefinition)
			);
		} else if (functionDefinition instanceof TableFunction) {
			final TableFunction<?> t = (TableFunction<?>) functionDefinition;
			return convertToSqlFunction(
				category,
				name,
				new TableFunctionDefinition(
					name,
					t,
					UserDefinedFunctionHelper.getReturnTypeOfTableFunction(t))
			);
		}
		throw new TableException(
			"The new type inference for functions is only supported in the Blink planner.");
	}

	private Optional<SqlFunction> convertAggregateFunction(
			String name,
			AggregateFunctionDefinition functionDefinition) {
		SqlFunction aggregateFunction = UserDefinedFunctionUtils.createAggregateSqlFunction(
			name,
			name,
			functionDefinition.getAggregateFunction(),
			functionDefinition.getResultTypeInfo(),
			functionDefinition.getAccumulatorTypeInfo(),
			typeFactory
		);
		return Optional.of(aggregateFunction);
	}

	private Optional<SqlFunction> convertScalarFunction(String name, ScalarFunctionDefinition functionDefinition) {
		SqlFunction scalarFunction = UserDefinedFunctionUtils.createScalarSqlFunction(
			name,
			name,
			functionDefinition.getScalarFunction(),
			typeFactory
		);
		return Optional.of(scalarFunction);
	}

	private Optional<SqlFunction> convertTableFunction(String name, TableFunctionDefinition functionDefinition) {
		SqlFunction tableFunction = UserDefinedFunctionUtils.createTableSqlFunction(
			name,
			name,
			functionDefinition.getTableFunction(),
			functionDefinition.getResultType(),
			typeFactory
		);
		return Optional.of(tableFunction);
	}

	@Override
	public List<SqlOperator> getOperatorList() {
		throw new UnsupportedOperationException("This should never be called");
	}
}
