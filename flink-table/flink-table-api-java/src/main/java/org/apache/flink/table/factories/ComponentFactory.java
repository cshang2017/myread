
package org.apache.flink.table.factories;

import org.apache.flink.annotation.PublicEvolving;

import java.util.List;
import java.util.Map;

/**
 * A factory interface for components that enables further disambiguating in case
 * there are multiple matching implementations present.
 */
@PublicEvolving
public interface ComponentFactory extends TableFactory {
	/**
	 * Specifies a context of optional parameters that if exist should have the
	 * given values. This enables further disambiguating if there are multiple
	 * factories that meet the {@link #requiredContext()} and {@link #supportedProperties()}.
	 *
	 * <p><b>NOTE:</b> All the property keys should be included in {@link #supportedProperties()}.
 	 *
	 * @return optional properties to disambiguate factories
	 */
	Map<String, String> optionalContext();

	@Override
	Map<String, String> requiredContext();

	/**
	 * {@inheritDoc}
	 *
	 * <p><b>NOTE:</b> All the property keys from {@link #optionalContext()} should also be included.
	 */
	@Override
	List<String> supportedProperties();
}
