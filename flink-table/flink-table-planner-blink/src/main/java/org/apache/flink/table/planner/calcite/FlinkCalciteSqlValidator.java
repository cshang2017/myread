package org.apache.flink.table.planner.calcite;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.DecimalType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Static;

import java.math.BigDecimal;
import java.util.List;

import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;

/**
 * Extends Calcite's {@link SqlValidator} by Flink-specific behavior.
 */
@Internal
public final class FlinkCalciteSqlValidator extends SqlValidatorImpl {

	public FlinkCalciteSqlValidator(
			SqlOperatorTable opTab,
			SqlValidatorCatalogReader catalogReader,
			RelDataTypeFactory typeFactory) {
		super(opTab, catalogReader, typeFactory, SqlConformanceEnum.DEFAULT);
	}

	@Override
	public void validateLiteral(SqlLiteral literal) {
		if (literal.getTypeName() == DECIMAL) {
			final BigDecimal decimal = literal.getValueAs(BigDecimal.class);
			if (decimal.precision() > DecimalType.MAX_PRECISION) {
				throw newValidationError(
					literal,
					Static.RESOURCE.numberLiteralOutOfRange(decimal.toString()));
			}
		}
		super.validateLiteral(literal);
	}

	@Override
	protected void validateJoin(SqlJoin join, SqlValidatorScope scope) {
		// Due to the improper translation of lateral table left outer join in Calcite, we need to
		// temporarily forbid the common predicates until the problem is fixed (see FLINK-7865).
		if (join.getJoinType() == JoinType.LEFT &&
				SqlUtil.stripAs(join.getRight()).getKind() == SqlKind.COLLECTION_TABLE) {
			final SqlNode condition = join.getCondition();
			if (condition != null &&
					(!SqlUtil.isLiteral(condition) || ((SqlLiteral) condition).getValueAs(Boolean.class) != Boolean.TRUE)) {
				throw new ValidationException(
					String.format(
						"Left outer joins with a table function do not accept a predicate such as %s. " +
						"Only literal TRUE is accepted.",
						condition));
			}
		}
		super.validateJoin(join, scope);
	}

	@Override
	public void validateColumnListParams(SqlFunction function, List<RelDataType> argTypes, List<SqlNode> operands) {
		// we don't support column lists and translate them into the unknown type in the type factory,
		// this makes it possible to ignore them in the validator and fall back to regular row types
		// see also SqlFunction#deriveType
	}
}
