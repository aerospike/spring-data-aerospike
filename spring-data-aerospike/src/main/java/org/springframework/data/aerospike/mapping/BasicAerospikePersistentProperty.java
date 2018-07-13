/*******************************************************************************
 * Copyright (c) 2015 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.springframework.data.aerospike.mapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.aerospike.annotation.Expiration;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.model.*;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BasicAerospikePersistentProperty extends AnnotationBasedPersistentProperty<AerospikePersistentProperty> implements
		AerospikePersistentProperty {

	private static final Logger LOG = LoggerFactory.getLogger(BasicAerospikePersistentProperty.class);

	private static final Set<Class<?>> SUPPORTED_ID_TYPES = new HashSet<Class<?>>();

	static {

		SUPPORTED_ID_TYPES.add(String.class);
		SUPPORTED_ID_TYPES.add(Integer.class);
		SUPPORTED_ID_TYPES.add(Long.class);
		SUPPORTED_ID_TYPES.add(byte[].class);
		SUPPORTED_ID_TYPES.add(Map.class);
		SUPPORTED_ID_TYPES.add(List.class);
	}

	private final FieldNamingStrategy fieldNamingStrategy;

	public BasicAerospikePersistentProperty(Field field,
											PropertyDescriptor propertyDescriptor,
											PersistentEntity<?, AerospikePersistentProperty> owner,
											SimpleTypeHolder simpleTypeHolder, FieldNamingStrategy fieldNamingStrategy) {
		super(field, propertyDescriptor, owner, simpleTypeHolder);

		this.fieldNamingStrategy = fieldNamingStrategy == null ? PropertyNameFieldNamingStrategy.INSTANCE
				: fieldNamingStrategy;
	}

	@Override
	public boolean isExplicitIdProperty() {
		return isAnnotationPresent(Id.class);
	}

	@Override
	public boolean isExpirationProperty() {
		return isAnnotationPresent(Expiration.class);
	}

	@Override
	public boolean isExpirationSpecifiedAsUnixTime() {
		Expiration expiration = findAnnotation(Expiration.class);
		Assert.state(expiration != null, "Property " + getName() + " is not expiration property");

		return expiration.unixTime();
	}

	/**
	 * @return the key to be used to store the value of the property
	 */
	public String getFieldName() {
		org.springframework.data.aerospike.mapping.Field annotation =
				findAnnotation(org.springframework.data.aerospike.mapping.Field.class);

		if (annotation != null && StringUtils.hasText(annotation.value())) {
			return annotation.value();
		}

		String fieldName = fieldNamingStrategy.getFieldName(this);

		if (!StringUtils.hasText(fieldName)) {
			throw new MappingException(String.format("Invalid (null or empty) field name returned for property %s by %s!",
					this, fieldNamingStrategy.getClass()));
		}

		return fieldName;
	}

	@Override
	protected Association<AerospikePersistentProperty> createAssociation() {
		return new Association<AerospikePersistentProperty>(this, null);
	}

}
