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
import org.springframework.data.util.Lazy;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Default implementation of {@link AerospikePersistentEntity}.
 *
 * @author Oliver Gierke
 */
public class BasicAerospikePersistentEntity<T>
    extends BasicPersistentEntity<T, AerospikePersistentProperty>
    implements AerospikePersistentEntity<T>, EnvironmentAware {

    static final int DEFAULT_EXPIRATION = 0;
    private final Lazy<String> setName;
    private final Lazy<Integer> expiration;
    private final Lazy<Boolean> isTouchOnRead;
    private AerospikePersistentProperty expirationProperty;
    private Environment environment;

    /**
     * Creates a new {@link BasicAerospikePersistentEntity} using a given {@link TypeInformation}.
     *
     * @param information must not be {@literal null}.
     */
    public BasicAerospikePersistentEntity(TypeInformation<T> information) {
        super(information);
        this.setName = Lazy.of(() -> {
            Class<T> type = getType();
            Document annotation = type.getAnnotation(Document.class);
            if (annotation != null && !annotation.collection().isEmpty()) {
                Assert.notNull(environment, "Environment must be set to use 'collection'");

                return environment.resolveRequiredPlaceholders(annotation.collection());
            }
            return type.getSimpleName();
        });
        this.expiration = Lazy.of(() -> {
            Document annotation = getType().getAnnotation(Document.class);
            if (annotation == null) {
                return DEFAULT_EXPIRATION;
            }

            int expirationValue = getExpirationValue(annotation);
            if (expirationValue <= 0) {
                return expirationValue;
            }

            return (int) annotation.expirationUnit().toSeconds(expirationValue);
        });
        this.isTouchOnRead = Lazy.of(() -> {
            Document annotation = getType().getAnnotation(Document.class);
            return annotation != null && annotation.touchOnRead();
        });
    }

    @Override
    public void addPersistentProperty(AerospikePersistentProperty property) {
        super.addPersistentProperty(property);

        if (property.isExpirationProperty()) {
            if (expirationProperty != null) {
                String message = String.format("Attempt to add expiration property %s but already have property %s " +
                        "registered as expiration. Check your mapping configuration!", property.getField(),
                    expirationProperty.getField());
                throw new MappingException(message);
            }

            expirationProperty = property;
        }
    }

    @Override
    public String getSetName() {
        return setName.get();
    }

    @Override
    public int getExpiration() {
        return expiration.get();
    }

    @Override
    public boolean isTouchOnRead() {
        return isTouchOnRead.get();
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
                throw new IllegalArgumentException(
                    "Invalid Integer value for expiration expression: " + resolvedExpression);
            }
        }

        return expiration;
    }
}
