package org.apache.calcite.sql.validate;

import org.apache.flink.annotation.Internal;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Namespace whose contents are defined by the result of a call to a user-defined procedure.
 *
 * <p>Note: Compared to Calcite, this class implements custom logic for dealing with collection tables
 * like {@code TABLE(function(...))} procedures. Compared to the SQL standard, Flink's table functions
 * can return arbitrary types that are wrapped into a ROW type if necessary. We don't interpret ARRAY
 * or MULTISET types as it would be standard.
 */
@Internal
public final class ProcedureNamespace extends AbstractNamespace {

	private final SqlValidatorScope scope;

	private final SqlCall call;

	ProcedureNamespace(
		SqlValidatorImpl validator,
		SqlValidatorScope scope,
		SqlCall call,
		SqlNode enclosingNode) {
		super(validator, enclosingNode);
		this.scope = scope;
		this.call = call;
	}

	public RelDataType validateImpl(RelDataType targetRowType) {
		validator.inferUnknownTypes(validator.unknownType, scope, call);
		final RelDataType type = validator.deriveTypeImpl(scope, call);
		final SqlOperator operator = call.getOperator();
		final SqlCallBinding callBinding =
			new SqlCallBinding(validator, scope, call);
		// legacy table functions
		if (operator instanceof SqlUserDefinedFunction) {
			assert type.getSqlTypeName() == SqlTypeName.CURSOR
				: "User-defined table function should have CURSOR type, not " + type;
			final SqlUserDefinedTableFunction udf =
				(SqlUserDefinedTableFunction) operator;
			return udf.getRowType(validator.typeFactory, callBinding.operands());
		}
		// special handling of collection tables TABLE(function(...))
		if (SqlUtil.stripAs(enclosingNode).getKind() == SqlKind.COLLECTION_TABLE) {
			return toStruct(type, getNode());
		}
		return type;
	}

	public SqlNode getNode() {
		return call;
	}
}
