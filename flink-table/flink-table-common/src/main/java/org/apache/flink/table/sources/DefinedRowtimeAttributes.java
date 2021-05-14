

package org.apache.flink.table.sources;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;

import java.util.List;

/**
 * Extends a {@link TableSource} to specify rowtime attributes via a
 * {@link RowtimeAttributeDescriptor}.
 */
@PublicEvolving
public interface DefinedRowtimeAttributes {

	/**
	 * Returns a list of {@link RowtimeAttributeDescriptor} for all rowtime
	 * attributes of the table.
	 *
	 * <p>All referenced attributes must be present in the {@link TableSchema}
	 * of the {@link TableSource} and of type {@link Types#SQL_TIMESTAMP}.
	 *
	 * @return A list of {@link RowtimeAttributeDescriptor}.
	 */
	List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors();
}
