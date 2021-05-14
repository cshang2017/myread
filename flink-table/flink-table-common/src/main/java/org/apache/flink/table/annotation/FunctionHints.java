package org.apache.flink.table.annotation;

import org.apache.flink.annotation.PublicEvolving;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Helper annotation for repeatable {@link FunctionHint}s.
 */
@PublicEvolving
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface FunctionHints {

	FunctionHint[] value();
}
