package org.springframework.data.aerospike.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Demarcates a property to be used as expiration field.
 * Expiration can be specified in two flavors: as an offset in seconds from the current time or as an absolute Unix time stamp.
 * <br/><br/>
 * Client system time must be synchronized with aerospike server system time,
 * otherwise expiration behaviour will be unpredictable.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Expiration {

	/**
	 * An optional flag indicating whether the expiration is specified as Unix time.
	 * By default an offset in seconds from the current time is used.
	 */
	boolean unixTime() default false;
}
