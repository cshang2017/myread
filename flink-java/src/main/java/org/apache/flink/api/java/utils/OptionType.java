

package org.apache.flink.api.java.utils;

/**
 * Types the parameters of managed with {@link RequiredParameters} can take.
 *
 * <p>Name maps directly to the corresponding Java type.
 *
 * @deprecated These classes will be dropped in the next version. Use {@link ParameterTool} or a third-party
 *             command line parsing library instead.
 */
@Deprecated
public enum OptionType {
	INTEGER,
	LONG,
	DOUBLE,
	FLOAT,
	BOOLEAN,
	STRING
}
