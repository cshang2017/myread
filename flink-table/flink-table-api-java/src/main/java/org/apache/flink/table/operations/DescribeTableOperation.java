package org.apache.flink.table.operations;

import org.apache.flink.table.catalog.ObjectIdentifier;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Operation to describe a DESCRIBE [EXTENDED] [[catalogName.] dataBasesName].sqlIdentifier statement.
 */
public class DescribeTableOperation implements Operation {

	private final ObjectIdentifier sqlIdentifier;
	private final boolean isExtended;

	public DescribeTableOperation(ObjectIdentifier sqlIdentifier, boolean isExtended) {
		this.sqlIdentifier = sqlIdentifier;
		this.isExtended = isExtended;
	}

	public ObjectIdentifier getSqlIdentifier() {
		return sqlIdentifier;
	}

	public boolean isExtended() {
		return isExtended;
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> params = new LinkedHashMap<>();
		params.put("identifier", sqlIdentifier);
		params.put("isExtended", isExtended);
		return OperationUtils.formatWithChildren(
			"DESCRIBE",
			params,
			Collections.emptyList(),
			Operation::asSummaryString);
	}
}
