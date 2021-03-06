package org.apache.flink.table.runtime.generated;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A wrapper for generated class, defines a {@link #newInstance(ClassLoader)} method
 * to get an instance by reference objects easily.
 */
public abstract class GeneratedClass<T> implements Serializable {

	private final String className;
	private final String code;
	private final Object[] references;

	private transient Class<T> compiledClass;

	protected GeneratedClass(String className, String code, Object[] references) {
		checkNotNull(className, "name must not be null");
		checkNotNull(code, "code must not be null");
		checkNotNull(references, "references must not be null");
		this.className = className;
		this.code = code;
		this.references = references;
	}

	/**
	 * Create a new instance of this generated class.
	 */
	@SuppressWarnings("unchecked")
	public T newInstance(ClassLoader classLoader) {
		try {
			return compile(classLoader).getConstructor(Object[].class)
					// Because Constructor.newInstance(Object... initargs), we need to load
					// references into a new Object[], otherwise it cannot be compiled.
					.newInstance(new Object[] {references});
		} catch (Exception e) {
			throw new RuntimeException(
				"Could not instantiate generated class '" + className + "'", e);
		}
	}

	@SuppressWarnings("unchecked")
	public T newInstance(ClassLoader classLoader, Object... args) {
		try {
			return (T) compile(classLoader).getConstructors()[0].newInstance(args);
		} catch (Exception e) {
			throw new RuntimeException(
					"Could not instantiate generated class '" + className + "'", e);
		}
	}

	/**
	 * Compiles the generated code, the compiled class will be cached in the {@link GeneratedClass}.
	 */
	public Class<T> compile(ClassLoader classLoader) {
		if (compiledClass == null) {
			// cache the compiled class
			compiledClass = CompileUtils.compile(classLoader, className, code);
		}
		return compiledClass;
	}

	public String getClassName() {
		return className;
	}

	public String getCode() {
		return code;
	}

	public Object[] getReferences() {
		return references;
	}

	public Class<T> getClass(ClassLoader classLoader) {
		return compile(classLoader);
	}
}
