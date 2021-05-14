

package org.apache.flink.api.java.utils;

import java.util.LinkedList;
import java.util.List;

/**
 * Exception which is thrown if validation of {@link RequiredParameters} fails.
 *
 * @deprecated These classes will be dropped in the next version. Use {@link ParameterTool} or a third-party
 *             command line parsing library instead.
 */
@Deprecated
public class RequiredParametersException extends Exception {

	private List<String> missingArguments;

	public RequiredParametersException() {
		super();
	}

	public RequiredParametersException(String message, List<String> missingArguments) {
		super(message);
		this.missingArguments = missingArguments;
	}

	public RequiredParametersException(String message) {
		super(message);
	}

	public RequiredParametersException(String message, Throwable cause) {
		super(message, cause);
	}

	public RequiredParametersException(Throwable cause) {
		super(cause);
	}

	public List<String> getMissingArguments() {
		if (missingArguments == null) {
			return new LinkedList<>();
		} else {
			return this.missingArguments;
		}
	}
}
