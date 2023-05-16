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

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.data.aerospike.annotation.Indexed;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.mapping.BasicAerospikePersistentEntity;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.StreamSupport;

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

        List<String> contexts = new ArrayList<>();
        int from = 0, to;
        if (ctxString.contains(".")) {
            while ((to = ctxString.indexOf('.', from)) >= 0) {
                String substr = ctxString.substring(from, to);
                if (substr.length() > 0) {
                    contexts.add(substr);
                }
                from = to + 1;
            }
        } else {
            contexts.add(ctxString);
        }

        return contexts.stream().map(this::toCtx).filter(Objects::nonNull).toArray(CTX[]::new);
    }

    private enum CtxType {
        MAP('}'), LIST(']');

        private final char closingChar;

        CtxType(char closingChar) {
            this.closingChar = closingChar;
        }

        @Override
        public String toString() {
            return name().toLowerCase(); // when mentioned in exceptions
        }
    }

    private CTX toCtx(String singleCtx) {
        switch (singleCtx.charAt(0)) {
            case '{' -> {
                return processSingleCtx(singleCtx, CtxType.MAP);
            }
            case '[' -> {
                return processSingleCtx(singleCtx, CtxType.LIST);
            }
            default -> {
                int length = singleCtx.length();
                Object res = isInDoubleQuotes(singleCtx) ? singleCtx.substring(1, length - 1) :
                    parseIntOrReturnStr(singleCtx);
                return CTX.mapKey(Value.get(res));
            }
        }
    }

    private CTX processSingleCtx(String singleCtx, CtxType ctxType) {
        String substr;
        Object res;
        int length = singleCtx.length();

        if (length < 3 || singleCtx.charAt(length - 1) != ctxType.closingChar) {
            return null;
        }

        if (singleCtx.charAt(1) == '=' && length > 3) {
            substr = singleCtx.substring(2, length - 1);
            res = isInDoubleQuotes(substr) ? substr.substring(1, substr.length() - 1) : parseIntOrReturnStr(substr);
            return switch (ctxType) {
                case MAP ->  CTX.mapValue(Value.get(res));
                case LIST ->  CTX.listValue(Value.get(res));
            };
        }

        if (singleCtx.charAt(1) == '#' && length > 3) {
            substr = singleCtx.substring(2, length - 1);
            int rank;
            try {
                rank = Integer.parseInt(substr);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("@Indexed annotation " + ctxType + " rank: " +
                    "expecting only integer values, got '" + substr + "' instead");
            }
            return switch (ctxType) {
                case MAP -> CTX.mapRank(rank);
                case LIST -> CTX.listRank(rank);
            };
        }

        substr = singleCtx.substring(1, length - 1);
        int idx;
        try {
            idx = Integer.parseInt(substr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("@Indexed annotation " + ctxType + " index: " +
                "expecting only integer values, got '" + substr + "' instead");
        }
        return switch (ctxType) {
            case MAP -> CTX.mapIndex(idx);
            case LIST -> CTX.listIndex(idx);
        };
    }

    private static Object parseIntOrReturnStr(String str) {
        Object res;
        try {
            res = Integer.parseInt(str);
        } catch (NumberFormatException e) {
            res = str;
        }

        return res;
    }

    private static boolean isInDoubleQuotes(String str) {
        return str.length() > 2 && str.charAt(0) == '"' && str.charAt(str.length() - 1) == '"';
    }
}
