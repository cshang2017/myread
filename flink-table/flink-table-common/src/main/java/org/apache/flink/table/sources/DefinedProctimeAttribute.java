
package org.apache.flink.table.sources;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;

import javax.annotation.Nullable;

/**
 * Extends a {@link TableSource} to specify a processing time attribute.
 */
@PublicEvolving
public interface DefinedProctimeAttribute {

		/**
		 * Returns the name of a processing time attribute or null if no processing time attribute is
		 * present.
		 *
		 * <p>The referenced attribute must be present in the {@link TableSchema} of the {@link TableSource} and of
		 * type {@link Types#SQL_TIMESTAMP}.
		 */
		@Nullable
		String getProctimeAttribute();
}
