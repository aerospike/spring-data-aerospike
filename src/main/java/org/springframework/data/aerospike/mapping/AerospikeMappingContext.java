/*******************************************************************************
 * Copyright (c) 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 * 		https://www.apache.org/licenses/LICENSE-2.0
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

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.mapping.context.AbstractMappingContext;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.FieldNamingStrategy;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.PropertyNameFieldNamingStrategy;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.TypeInformation;

/**
 * An Aerospike-specific implementation of {@link MappingContext}.
 * 
 * @author Oliver Gierke
 * @author Peter Milne
 */
public class AerospikeMappingContext extends
		AbstractMappingContext<BasicAerospikePersistentEntity<?>, AerospikePersistentProperty> implements ApplicationContextAware{

	private static final FieldNamingStrategy DEFAULT_NAMING_STRATEGY = PropertyNameFieldNamingStrategy.INSTANCE;

	private FieldNamingStrategy fieldNamingStrategy = DEFAULT_NAMING_STRATEGY;
	private ApplicationContext context;
	private String defaultNameSpace;
	private boolean createIndexesOnStartup;

	/**
	 * Configures the {@link FieldNamingStrategy} to be used to determine the field name if no manual mapping is applied.
	 * Defaults to a strategy using the plain property name.
	 * 
	 * @param fieldNamingStrategy the {@link FieldNamingStrategy} to be used to determine the field name if no manual
	 *          mapping is applied.
	 */
	public void setFieldNamingStrategy(FieldNamingStrategy fieldNamingStrategy) {
		this.fieldNamingStrategy = fieldNamingStrategy == null ? DEFAULT_NAMING_STRATEGY : fieldNamingStrategy;
	}
	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#createPersistentEntity(org.springframework.data.util.TypeInformation)
	 */
	@Override
	protected <T> BasicAerospikePersistentEntity<?> createPersistentEntity(TypeInformation<T> typeInformation) {
		BasicAerospikePersistentEntity<T> entity = new  BasicAerospikePersistentEntity<T>(typeInformation, defaultNameSpace);
		if (context != null) {
			entity.setEnvironment(context.getEnvironment());
		}
		return entity;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#createPersistentProperty(java.lang.reflect.Field, java.beans.PropertyDescriptor, org.springframework.data.mapping.model.MutablePersistentEntity, org.springframework.data.mapping.model.SimpleTypeHolder)
	 */
	@Override
	protected AerospikePersistentProperty createPersistentProperty(Property property,
		BasicAerospikePersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder) {
		return new CachingAerospikePersistentProperty(property, owner, simpleTypeHolder, fieldNamingStrategy);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.context = applicationContext;
	}

	public void setDefaultNameSpace(String defaultNameSpace) {
		this.defaultNameSpace = defaultNameSpace;
	}

	public void setCreateIndexesOnStartup(boolean createIndexesOnStartup) {
		this.createIndexesOnStartup = createIndexesOnStartup;
	}

	public boolean isCreateIndexesOnStartup() {
		return createIndexesOnStartup;
	}
}
