/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.mapping;

import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Default implementation of {@link AerospikePersistentEntity}.
 * 
 * @author Oliver Gierke
 */
public class BasicAerospikePersistentEntity<T> extends BasicPersistentEntity<T, AerospikePersistentProperty> implements
		AerospikePersistentEntity<T>, EnvironmentAware {

	static final int DEFAULT_EXPIRATION = 0;

	private final String defaultNameSpace;

	private AerospikePersistentProperty expirationProperty;
	private Environment environment;

	/**
	 * Creates a new {@link BasicAerospikePersistentEntity}
	 * using a given {@link TypeInformation} and a default namespace.
	 *
	 * @param information must not be {@literal null}.
	 * @param defaultNameSpace The default namespace.
	 */
	public BasicAerospikePersistentEntity(TypeInformation<T> information, String defaultNameSpace) {
		super(information);
		this.defaultNameSpace = defaultNameSpace;
	}

	@Override
	public void addPersistentProperty(AerospikePersistentProperty property) {
		super.addPersistentProperty(property);

		if (property.isExpirationProperty()) {
			if (expirationProperty != null) {
				String message = String.format("Attempt to add expiration property %s but already have property %s " +
						"registered as expiration. Check your mapping configuration!", property.getField(), expirationProperty.getField());
				throw new MappingException(message);
			}

			expirationProperty = property;
		}
	}

	@Override
	public String getNamespace() {
		return defaultNameSpace;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.aerospike.mapping.AerospikePersistentEntity#getSetName()
	 */
	@Override
	public String getSetName() {
		Class<T> type = getType();
		Document annotation = type.getAnnotation(Document.class);
		if (annotation != null && !annotation.collection().isEmpty()) {
			Assert.notNull(environment, "Environment must be set to use 'collection'");

			return environment.resolveRequiredPlaceholders(annotation.collection());
		}
		return type.getSimpleName();
	}

	@Override
	public int getExpiration() {
		Document annotation = getType().getAnnotation(Document.class);
		if (annotation == null) {
			return DEFAULT_EXPIRATION;
		}

		int expirationValue = getExpirationValue(annotation);
		return (int) annotation.expirationUnit().toSeconds(expirationValue);
	}

	@Override
	public boolean isTouchOnRead() {
		Document annotation = getType().getAnnotation(Document.class);
		return annotation != null && annotation.touchOnRead();
	}

	@Override
	public AerospikePersistentProperty getExpirationProperty() {
		return expirationProperty;
	}

	@Override
	public boolean hasExpirationProperty() {
		return expirationProperty != null;
	}

	@Override
	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}

	private int getExpirationValue(Document annotation) {
		int expiration = annotation.expiration();
		String expressionString = annotation.expirationExpression();

		if (StringUtils.hasLength(expressionString)) {
			Assert.state(expiration == DEFAULT_EXPIRATION, "Both 'expiration' and 'expirationExpression' are set");
			Assert.notNull(environment, "Environment must be set to use 'expirationExpression'");

			String resolvedExpression = environment.resolveRequiredPlaceholders(expressionString);
			try {
				return Integer.parseInt(resolvedExpression);
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("Invalid Integer value for expiration expression: " + resolvedExpression);
			}
		}

		return expiration;
	}
}
