package org.apache.flink.streaming.api.functions.windowing.delta.extractor;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

/**
 * Extractors allow to extract/convert one type to another. They are mostly used
 * to extract some fields out of a more complex structure (Tuple/Array) to run
 * further calculation on the extraction result.
 *
 * @param <FROM>
 *            The input data type.
 * @param <TO>
 *            The output data type.
 */
@Internal
public interface Extractor<FROM, TO> extends Serializable {

	/**
	 * Extracts/Converts the given input to an object of the output type.
	 *
	 * @param in
	 *            the input data
	 * @return the extracted/converted data
	 */
	TO extract(FROM in);

}
