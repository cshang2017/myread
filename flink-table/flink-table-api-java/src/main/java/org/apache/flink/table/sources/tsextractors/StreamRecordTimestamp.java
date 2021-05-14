package org.apache.flink.table.sources.tsextractors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedFieldReference;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.STREAM_RECORD_TIMESTAMP;

/**
 * Extracts the timestamp of a StreamRecord into a rowtime attribute.
 *
 * <p>Note: This extractor only works for StreamTableSources.
 */
@PublicEvolving
public final class StreamRecordTimestamp extends TimestampExtractor {

	private static final long serialVersionUID = 1L;

	public static final StreamRecordTimestamp INSTANCE = new StreamRecordTimestamp();

	@Override
	public String[] getArgumentFields() {
		return new String[0];
	}

	@Override
	public void validateArgumentFields(TypeInformation<?>[] argumentFieldTypes) {
	}

	@Override
	public Expression getExpression(ResolvedFieldReference[] fieldAccesses) {
		return ApiExpressionUtils.unresolvedCall(STREAM_RECORD_TIMESTAMP);
	}

	@Override
	public Map<String, String> toProperties() {
		Map<String, String> map = new HashMap<>();
		map.put(Rowtime.ROWTIME_TIMESTAMPS_TYPE, Rowtime.ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_SOURCE);
		return map;
	}

}
