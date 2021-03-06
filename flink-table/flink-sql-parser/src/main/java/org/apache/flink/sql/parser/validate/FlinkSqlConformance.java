package org.apache.flink.sql.parser.validate;

import org.apache.calcite.sql.validate.SqlConformance;

/** Sql conformance used for flink to set specific sql dialect parser. **/
public enum FlinkSqlConformance implements SqlConformance {
	/** Calcite's default SQL behavior. */
	DEFAULT,

	/** Conformance value that instructs Calcite to use SQL semantics
	 * consistent with the Apache HIVE, but ignoring its more
	 * inconvenient or controversial dicta. */
	HIVE;

	@Override
	public boolean isLiberal() {
		return false;
	}

	@Override
	public boolean isGroupByAlias() {
		return false;
	}

	@Override
	public boolean isGroupByOrdinal() {
		return false;
	}

	@Override
	public boolean isHavingAlias() {
		return false;
	}

	@Override
	public boolean isSortByOrdinal() {
		switch (this) {
			case DEFAULT:
			case HIVE:
				return true;
			default:
				return false;
		}
	}

	@Override
	public boolean isSortByAlias() {
		switch (this) {
			case DEFAULT:
			case HIVE:
				return true;
			default:
				return false;
		}
	}

	@Override
	public boolean isSortByAliasObscures() {
		return false;
	}

	@Override
	public boolean isFromRequired() {
		return false;
	}

	@Override
	public boolean isBangEqualAllowed() {
		return false;
	}

	@Override
	public boolean isPercentRemainderAllowed() {
		return false;
	}

	@Override
	public boolean isMinusAllowed() {
		return false;
	}

	@Override
	public boolean isApplyAllowed() {
		return false;
	}

	@Override
	public boolean isInsertSubsetColumnsAllowed() {
		return false;
	}

	@Override
	public boolean allowNiladicParentheses() {
		return false;
	}

	@Override
	public boolean allowExplicitRowValueConstructor() {
		switch (this) {
		case DEFAULT:
			return true;
		}
		return false;
	}

	@Override
	public boolean allowExtend() {
		return false;
	}

	@Override
	public boolean isLimitStartCountAllowed() {
		return false;
	}

	@Override
	public boolean allowGeometry() {
		return false;
	}

	@Override
	public boolean shouldConvertRaggedUnionTypesToVarying() {
		return false;
	}

	@Override
	public boolean allowExtendedTrim() {
		return false;
	}

	@Override
	public boolean allowPluralTimeUnits() {
		return false;
	}

	@Override
	public boolean allowQualifyingCommonColumn() {
		return true;
	}
}
