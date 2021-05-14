package org.apache.flink.table.sources.tsextractors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedFieldReference;
import org.apache.flink.table.types.DataType;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.expressions.ApiExpressionUtils.typeLiteral;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.CAST;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Converts an existing {@link Long}, {@link java.sql.Timestamp}, or
 * timestamp formatted java.lang.String field (e.g., "2018-05-28 12:34:56.000") into
 * a rowtime attribute.
 */
@PublicEvolving
public final class ExistingField extends TimestampExtractor {

	private String field;

	/**
	 * @param field The field to convert into a rowtime attribute.
	 */
	public ExistingField(String field) {
		this.field = checkNotNull(field);
	}

	@Override
	public String[] getArgumentFields() {
		return new String[] {field};
	}

	@Override
	public void validateArgumentFields(TypeInformation<?>[] argumentFieldTypes) {
		DataType fieldType = fromLegacyInfoToDataType(argumentFieldTypes[0]);

		switch (fieldType.getLogicalType().getTypeRoot()) {
			case BIGINT:
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case VARCHAR:
				break;
			default:
				throw new ValidationException(String.format(
						"Field '%s' must be of type Long or Timestamp or String but is of type %s.",
						field, fieldType));
		}
	}

	/**
	 * Returns an {@link Expression} that casts a {@link Long}, {@link Timestamp}, or
	 * timestamp formatted {@link String} field (e.g., "2018-05-28 12:34:56.000")
	 * into a rowtime attribute.
	 */
	@Override
	public Expression getExpression(ResolvedFieldReference[] fieldAccesses) {
		ResolvedFieldReference fieldAccess = fieldAccesses[0];
		DataType type = fromLegacyInfoToDataType(fieldAccess.resultType());

		FieldReferenceExpression fieldReferenceExpr = new FieldReferenceExpression(
				fieldAccess.name(),
				type,
				0,
				fieldAccess.fieldIndex());

		switch (type.getLogicalType().getTypeRoot()) {
			case BIGINT:
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return fieldReferenceExpr;
			case VARCHAR:
				DataType outputType = TIMESTAMP(3).bridgedTo(Timestamp.class);
				return new CallExpression(
						CAST,
						Arrays.asList(fieldReferenceExpr, typeLiteral(outputType)),
						outputType);
			default:
				throw new RuntimeException("Unsupport type: " + type);
		}
	}

	@Override
	public Map<String, String> toProperties() {
		Map<String, String> map = new HashMap<>();
		map.put(Rowtime.ROWTIME_TIMESTAMPS_TYPE, Rowtime.ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD);
		map.put(Rowtime.ROWTIME_TIMESTAMPS_FROM, field);
		return map;
	}


}
