/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.core.model;


import lombok.Builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@Builder
public class GroupedEntities {

    private final Map<Class<?>, List<?>> entitiesResults;

    @SuppressWarnings("unchecked")
    public <T> List<T> getEntitiesByClass(Class<T> entityClass) {
        return (List<T>) entitiesResults.getOrDefault(entityClass, emptyList());
    }

    public boolean containsEntities() {
        return entitiesResults.entrySet().stream().anyMatch(entry -> !entry.getValue().isEmpty());
    }

    public static class GroupedEntitiesBuilder {

        private Map<Class<?>, List<?>> entitiesResults = new HashMap<>();

        @SuppressWarnings("unchecked")
        public <T> GroupedEntities.GroupedEntitiesBuilder entity(Class<T> key, T entity) {
            entitiesResults.compute(key, (k,v) -> {
                if (v == null) {
                    return new ArrayList<>(singletonList(entity));
                }

                ((List<T>)v).add(entity);
                return v;
            });

            return this;
        }

        private GroupedEntities.GroupedEntitiesBuilder entitiesResults(Map<Class<?>, List<?>> keysMap) {
            this.entitiesResults = keysMap;
            return this;
        }
    }
}
