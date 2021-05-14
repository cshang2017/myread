package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;

/**
 * Validator of python function.
 */
@Internal
public class PythonFunctionValidator implements DescriptorValidator {

	public static final String FULLY_QUALIFIED_NAME = "fully-qualified-name";

	@Override
	public void validate(DescriptorProperties properties) {
		properties.validateString(FULLY_QUALIFIED_NAME, false);
	}
}
