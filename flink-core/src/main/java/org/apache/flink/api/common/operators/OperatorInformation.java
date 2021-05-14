
package org.apache.flink.api.common.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 *  A class for holding information about an operator, such as input/output TypeInformation.
 *
 * @param <OUT> Output type of the records output by the operator described by this information
*/
@Internal
public class OperatorInformation<OUT> {
	/**
	 * Output type of the operator
	 */
	protected final TypeInformation<OUT> outputType;

	/**
	 * @param outputType The output type of the operator
	 */
	public OperatorInformation(TypeInformation<OUT> outputType) {
		this.outputType = outputType;
	}

	/**
	 * Gets the return type of the user code function.
	 */
	public TypeInformation<OUT> getOutputType() {
		return outputType;
	}


	@Override
	public String toString() {
		return "Operator Info; Output type: " + outputType;
	}
}
