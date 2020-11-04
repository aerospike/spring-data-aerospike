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

import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.mapping.BasicAerospikePersistentEntity;
import org.springframework.util.StringUtils;

import java.util.Set;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toSet;

/**
 * @author Taras Danylchuk
 */
public class AerospikeIndexResolver {

    public Set<AerospikeIndexDefinition> detectIndexes(BasicAerospikePersistentEntity<?> persistentEntity) {
        return StreamSupport.stream(persistentEntity.spliterator(), false)
                .filter(property -> property.isAnnotationPresent(Indexed.class))
                .map(property -> convertToIndex(persistentEntity, property))
                .collect(toSet());
    }

    private AerospikeIndexDefinition convertToIndex(BasicAerospikePersistentEntity<?> persistentEntity,
                                                    AerospikePersistentProperty property) {
        Indexed annotation = property.getRequiredAnnotation(Indexed.class);
        String indexName = StringUtils.isEmpty(annotation.name())
                ? String.join("_", persistentEntity.getSetName(), property.getFieldName())
                : annotation.name();
        return AerospikeIndexDefinition.builder()
                .entityClass(persistentEntity.getType())
                .fieldName(property.getFieldName())
                .name(indexName)
                .type(annotation.type())
                .build();
    }
}
