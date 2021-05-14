package org.apache.flink.api.common.typeinfo;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Type;

/**
 * Annotation for specifying a corresponding {@link TypeInfoFactory} that can produce
 * {@link TypeInformation} for the annotated type. In a hierarchy of types the closest annotation
 * that defines a factory will be chosen while traversing upwards, however, a globally registered
 * factory has highest precedence (see {@link TypeExtractor#registerFactory(Type, Class)}).
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Public
public @interface TypeInfo {

	Class<? extends TypeInfoFactory> value();

}
