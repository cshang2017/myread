
package org.apache.flink.api.common.typeinfo;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.FlinkRuntimeException;

/**
 * A utility class for describing generic types. It can be used to obtain a type information via:
 *
 * <pre>{@code
 * TypeInformation<Tuple2<String, Long>> info = TypeInformation.of(new TypeHint<Tuple2<String, Long>>(){});
 * }</pre>
 * or
 * <pre>{@code
 * TypeInformation<Tuple2<String, Long>> info = new TypeHint<Tuple2<String, Long>>(){}.getTypeInfo();
 * }</pre>
 *
 * @param <T> The type information to hint.
 */
@Public
public abstract class TypeHint<T> {

	/** The type information described by the hint. */
	private final TypeInformation<T> typeInfo;

	/**
	 * Creates a hint for the generic type in the class signature.
	 */
	public TypeHint() {
			this.typeInfo = TypeExtractor.createTypeInfo(
					this, TypeHint.class, getClass(), 0);
		
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the type information described by this TypeHint.
	 * @return The type information described by this TypeHint.
	 */
	public TypeInformation<T> getTypeInfo() {
		return typeInfo;
	}



	@Override
	public String toString() {
		return "TypeHint: " + typeInfo;
	}
}
