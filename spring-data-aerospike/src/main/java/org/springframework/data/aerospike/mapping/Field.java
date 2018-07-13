/**
 * 
 */
package org.springframework.data.aerospike.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to define custom metadata for document fields.
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Field {
	/**
	 * The key to be used to store the field inside the document.
	 * 
	 * @return
	 */
	String value() default "";

}
