package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;
import java.util.Map;

/**
 * Descriptor for describing a function.
 */
@PublicEvolving
public class FunctionDescriptor implements Descriptor {
	private String from;
	private ClassInstance classInstance;
	private String fullyQualifiedName;

	/**
	 * Creates a function from a class description.
	 */
	public FunctionDescriptor fromClass(ClassInstance classType) {
		from = FunctionDescriptorValidator.FROM_VALUE_CLASS;
		this.classInstance = classType;
		this.fullyQualifiedName = null;
		return this;
	}

	/**
	 * Converts this descriptor into a set of properties.
	 */
	@Override
	public Map<String, String> toProperties() {
		DescriptorProperties properties = new DescriptorProperties();
		if (from != null) {
			properties.putString(FunctionDescriptorValidator.FROM, from);
		}
		if (classInstance != null) {
			properties.putProperties(classInstance.toProperties());
		}
		if (fullyQualifiedName != null) {
			properties.putString(PythonFunctionValidator.FULLY_QUALIFIED_NAME, fullyQualifiedName);
		}
		return properties.asMap();
	}
}
