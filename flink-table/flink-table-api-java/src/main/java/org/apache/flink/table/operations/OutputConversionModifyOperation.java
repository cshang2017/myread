package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.types.DataType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Special, internal kind of {@link ModifyOperation} that allows converting a tree of
 * {@link QueryOperation}s to a {@link Transformation} of given type described with
 * {@link TypeInformation}. This is used to convert a relational query to a datastream.
 */
@Internal
public class OutputConversionModifyOperation implements ModifyOperation {

	/**
	 * Should the output type contain the change flag, and what should the
	 * flag represent (retraction or deletion).
	 */
	public enum UpdateMode {
		APPEND,
		RETRACT,
		UPSERT
	}

	private final QueryOperation child;
	private final DataType type;
	private final UpdateMode updateMode;

	public OutputConversionModifyOperation(QueryOperation child, DataType type, UpdateMode updateMode) {
		this.child = child;
		this.type = type;
		this.updateMode = updateMode;
	}

	public UpdateMode getUpdateMode() {
		return updateMode;
	}

	public DataType getType() {
		return type;
	}

	@Override
	public QueryOperation getChild() {
		return child;
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> params = new LinkedHashMap<>();
		params.put("updateMode", updateMode);
		params.put("type", type);

		return OperationUtils.formatWithChildren(
			"Output",
			params,
			Collections.singletonList(child),
			Operation::asSummaryString);
	}

	@Override
	public <R> R accept(ModifyOperationVisitor<R> visitor) {
		return visitor.visit(this);
	}
}
