/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.aerospike.index;

import com.aerospike.client.cdt.CTX;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.data.aerospike.annotation.Indexed;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.mapping.BasicAerospikePersistentEntity;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.StreamSupport;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toSet;

/**
 * @author Taras Danylchuk
 */
public class AerospikeIndexResolver implements EnvironmentAware {

    private Environment environment;

    public Set<AerospikeIndexDefinition> detectIndexes(BasicAerospikePersistentEntity<?> persistentEntity) {
        return StreamSupport.stream(persistentEntity.spliterator(), false)
            .filter(property -> property.isAnnotationPresent(Indexed.class))
            .map(property -> convertToIndex(persistentEntity, property))
            .collect(toSet());
    }

    private AerospikeIndexDefinition convertToIndex(BasicAerospikePersistentEntity<?> persistentEntity,
                                                    AerospikePersistentProperty property) {
        Indexed annotation = property.getRequiredAnnotation(Indexed.class);
        String indexName;
        if (StringUtils.hasText(annotation.name())) {
            Assert.notNull(environment, "Environment must be set to use 'indexed'");
            indexName = environment.resolveRequiredPlaceholders(annotation.name());
        } else {
            indexName = getIndexName(persistentEntity, property, annotation);
        }

        String binName;
        if (StringUtils.hasText(annotation.bin())) {
            binName = annotation.bin();
        } else {
            // using the name of the annotated field if bin name is not provided
            binName = property.getFieldName();
        }

        return AerospikeIndexDefinition.builder()
            .entityClass(persistentEntity.getType())
            .bin(binName)
            .name(indexName)
            .type(annotation.type())
            .collectionType(annotation.collectionType())
            .ctx(toCtxArray(annotation.ctx()))
            .build();
    }

    private String getIndexName(BasicAerospikePersistentEntity<?> entity,
                                AerospikePersistentProperty property, Indexed annotation) {
        return String.join("_",
            entity.getSetName(), property.getFieldName(), annotation.type().name().toLowerCase(),
            annotation.collectionType().name().toLowerCase());
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    private CTX[] toCtxArray(String ctxString) {
        if (!StringUtils.hasLength(ctxString)) return null;

        String[] ctxTokens = ctxString.split("\\.");
        CTX[] ctxArr = Arrays.stream(ctxTokens)
            .filter(not(String::isEmpty))
            .map(AerospikeContextDslResolverUtils::toCtx)
            .filter(Objects::nonNull)
            .toArray(CTX[]::new);

        if (ctxTokens.length != ctxArr.length) {
            throw new IllegalArgumentException("@Indexed annotation '" + ctxString + "' contains empty context");
        }

        return ctxArr;
    }

    protected enum CtxType {
        MAP('}'), LIST(']');

        final char closingChar;

        CtxType(char closingChar) {
            this.closingChar = closingChar;
        }

        @Override
        public String toString() {
            return name().toLowerCase(); // when mentioned in exceptions
        }
    }
}
