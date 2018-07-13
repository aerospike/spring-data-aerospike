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

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;

import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.model.FieldNamingStrategy;
import org.springframework.data.mapping.model.SimpleTypeHolder;

/**
 * 
 * @author Peter Milne
 *
 */
public class CachingAerospikePersistentProperty extends BasicAerospikePersistentProperty{

	private Boolean isIdProperty;
	private Boolean isAssociation;
	private String fieldName;
	private Boolean usePropertyAccess;
	private Boolean isTransient;
	private Boolean isExpirationProperty;
	private Boolean isExpirationSpecifiedAsUnixTime;

	/**
	 * Creates a new {@link CachingAerospikePersistentProperty}.
	 * 
	 * @param field
	 * @param propertyDescriptor
	 * @param owner
	 * @param simpleTypeHolder
	 * @param fieldNamingStrategy
	 */
	public CachingAerospikePersistentProperty(Field field,
			PropertyDescriptor propertyDescriptor,
			PersistentEntity<?, AerospikePersistentProperty> owner,
			SimpleTypeHolder simpleTypeHolder,
			FieldNamingStrategy fieldNamingStrategy) {
		super(field, propertyDescriptor, owner, simpleTypeHolder, fieldNamingStrategy);
	}

	@Override
	public boolean isIdProperty() {

		if (this.isIdProperty == null) {
			this.isIdProperty = super.isIdProperty();
		}

		return this.isIdProperty;
	}

	@Override
	public boolean isAssociation() {
		if (this.isAssociation == null) {
			this.isAssociation = super.isAssociation();
		}
		return this.isAssociation;
	}

	@Override
	public String getFieldName() {

		if (this.fieldName == null) {
			this.fieldName = super.getFieldName();
		}

		return this.fieldName;
	}

	@Override
	public boolean usePropertyAccess() {

		if (this.usePropertyAccess == null) {
			this.usePropertyAccess = super.usePropertyAccess();
		}

		return this.usePropertyAccess;
	}

	@Override
	public boolean isTransient() {

		if (this.isTransient == null) {
			this.isTransient = super.isTransient();
		}

		return this.isTransient;
	}

	@Override
	public boolean isExpirationProperty() {
		if (this.isExpirationProperty == null) {
			this.isExpirationProperty = super.isExpirationProperty();
		}
		return this.isExpirationProperty;
	}

	@Override
	public boolean isExpirationSpecifiedAsUnixTime() {
		if (this.isExpirationSpecifiedAsUnixTime == null) {
			this.isExpirationSpecifiedAsUnixTime = super.isExpirationSpecifiedAsUnixTime();
		}

		return this.isExpirationSpecifiedAsUnixTime;
	}
}
