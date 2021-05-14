package org.apache.flink.table.planner.functions.inference;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.inference.ArgumentCount;

import org.apache.calcite.sql.SqlOperandCountRange;

/**
 * A {@link SqlOperandCountRange} backed by {@link ArgumentCount}.
 */
@Internal
public final class ArgumentCountRange implements SqlOperandCountRange {

	private final ArgumentCount argumentCount;

	public ArgumentCountRange(ArgumentCount argumentCount) {
		this.argumentCount = argumentCount;
	}

	@Override
	public boolean isValidCount(int count) {
		return argumentCount.isValidCount(count);
	}

	@Override
	public int getMin() {
		return argumentCount.getMinCount().orElse(-1);
	}

	@Override
	public int getMax() {
		return argumentCount.getMaxCount().orElse(-1);
	}
}
