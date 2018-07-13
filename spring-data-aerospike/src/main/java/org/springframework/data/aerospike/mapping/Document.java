/*******************************************************************************
 * Copyright (c) 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *  	
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.springframework.data.aerospike.mapping;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

import org.springframework.data.annotation.Persistent;

import static org.springframework.data.aerospike.mapping.BasicAerospikePersistentEntity.DEFAULT_EXPIRATION;

/**
 * Identifies a domain object to be persisted to Aerospike.
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
@Persistent
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface Document {

	//TODO: add support for SPEL expression
	String collection() default "";

	/**
	 * Defines the default language to be used with this document.
	 * 
	 * @since 1.6
	 * @return
	 */
	String language() default "";

	/**
	 * An optional expiration time for the document. Default is no expiration. Ignored if entity has field
	 * annotated by {@link org.springframework.data.aerospike.annotation.Expiration}
	 * <br/>
	 * Only one of two might might be set at the same time: either {@link #expiration()} or {@link #expirationExpression()}
	 * See {@link com.aerospike.client.policy.WritePolicy#expiration} for possible values.
	 */
	int expiration() default DEFAULT_EXPIRATION;

	/**
	 * Same as {@link #expiration} but allows the actual value to be set using standard Spring property sources mechanism.
	 * Only one might be set at the same time: either {@link #expiration()} or {@link #expirationExpression()}. <br />
	 * Syntax is the same as for {@link org.springframework.core.env.Environment#resolveRequiredPlaceholders(String)}.
	 * <br /><br />
	 * SpEL is NOT supported.
	 */
	String expirationExpression() default "";

	/**
	 * An optional time unit for the document's {@link #expiration()}, if set. Default is {@link TimeUnit#SECONDS}.
	 */
	TimeUnit expirationUnit() default TimeUnit.SECONDS;

	/**
	 * An optional flag associated indicating whether the expiration timer should be reset whenever the document is directly read
	 */
	boolean touchOnRead() default false;
}
