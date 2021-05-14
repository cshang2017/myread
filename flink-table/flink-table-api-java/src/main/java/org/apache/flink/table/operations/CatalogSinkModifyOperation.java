

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.ObjectIdentifier;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * DML operation that tells to write to a sink. The sink has to be looked up in a
 * {@link org.apache.flink.table.catalog.Catalog}.
 */
@Internal
public class CatalogSinkModifyOperation implements ModifyOperation {

	private final ObjectIdentifier tableIdentifier;
	private final Map<String, String> staticPartitions;
	private final QueryOperation child;
	private final boolean overwrite;
	private final Map<String, String> dynamicOptions;

	public CatalogSinkModifyOperation(ObjectIdentifier tableIdentifier, QueryOperation child) {
		this(tableIdentifier, child, Collections.emptyMap(), false, Collections.emptyMap());
	}

	public CatalogSinkModifyOperation(
			ObjectIdentifier tableIdentifier,
			QueryOperation child,
			Map<String, String> staticPartitions,
			boolean overwrite,
			Map<String, String> dynamicOptions) {
		this.tableIdentifier = tableIdentifier;
		this.child = child;
		this.staticPartitions = staticPartitions;
		this.overwrite = overwrite;
		this.dynamicOptions = dynamicOptions;
	}

	public ObjectIdentifier getTableIdentifier() {
		return tableIdentifier;
	}

	public Map<String, String> getStaticPartitions() {
		return staticPartitions;
	}

	public boolean isOverwrite() {
		return overwrite;
	}

	public Map<String, String> getDynamicOptions() {
		return dynamicOptions;
	}

	@Override
	public QueryOperation getChild() {
		return child;
	}

	@Override
	public <T> T accept(ModifyOperationVisitor<T> visitor) {
		return visitor.visit(this);
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> params = new LinkedHashMap<>();
		params.put("identifier", tableIdentifier);
		params.put("staticPartitions", staticPartitions);
		params.put("overwrite", overwrite);
		params.put("dynamicOptions", dynamicOptions);

		return OperationUtils.formatWithChildren(
			"CatalogSink",
			params,
			Collections.singletonList(child),
			Operation::asSummaryString);
	}
}
