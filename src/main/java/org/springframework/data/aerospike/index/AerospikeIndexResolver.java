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
        CTX[] ctxArr = Arrays.stream(ctxTokens).filter(not(String::isEmpty))
            .map(this::toCtx).filter(Objects::nonNull).toArray(CTX[]::new);

        if (ctxTokens.length != ctxArr.length) {
            throw new IllegalArgumentException("@Indexed annotation '" + ctxString + "' contains empty context");
        }

        return ctxArr;
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
                Object res = isInDoubleOrSingleQuotes(singleCtx) ? singleCtx.substring(1, singleCtx.length() - 1) :
                    parseIntOrReturnStr(singleCtx);
                return CTX.mapKey(Value.get(res));
            }
        }
    }

    private CTX processSingleCtx(String singleCtx, CtxType ctxType) {
        int length = singleCtx.length();
        if (length < 3) {
            throw new IllegalArgumentException("@Indexed annotation: context string '" + singleCtx +
                "' has no content");
        }
        if (singleCtx.charAt(length - 1) != ctxType.closingChar) {
            throw new IllegalArgumentException("@Indexed annotation: brackets mismatch, " +
                "expecting '" + ctxType.closingChar + "', got '" + singleCtx.charAt(length - 1) + "' instead");
        }

        CTX result;
        String substring = singleCtx.substring(2, length - 1);
        if (singleCtx.charAt(1) == '=' && length > 3) {
            result = processCtxValue(substring, ctxType);
        } else if (singleCtx.charAt(1) == '#' && length > 3) {
            result = processCtxRank(substring, ctxType);
        } else {
            result = processCtxIndex(singleCtx, length, ctxType);
        }

        return result;
    }

    private CTX processCtxValue(String substring, CtxType ctxType) {
        Object result = isInDoubleOrSingleQuotes(substring) ? substring.substring(1, substring.length() - 1) :
            parseIntOrReturnStr(substring);
        return switch (ctxType) {
            case MAP -> CTX.mapValue(Value.get(result));
            case LIST -> CTX.listValue(Value.get(result));
        };
    }

    private CTX processCtxRank(String substring, CtxType ctxType) {
        int rank = parseIntOrFail(substring, ctxType, "rank");
        return switch (ctxType) {
            case MAP -> CTX.mapRank(rank);
            case LIST -> CTX.listRank(rank);
        };
    }

    private CTX processCtxIndex(String singleCtx, int length, CtxType ctxType) {
        String substring = singleCtx.substring(1, length - 1);
        int idx = parseIntOrFail(substring, ctxType, "index");
        return switch (ctxType) {
            case MAP -> CTX.mapIndex(idx);
            case LIST -> CTX.listIndex(idx);
        };
    }

    private int parseIntOrFail(String substring, CtxType ctxType, String parameterName) {
        try {
            return Integer.parseInt(substring);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("@Indexed annotation " + ctxType + " " + parameterName + ": " +
                "expecting only integer values, got '" + substring + "' instead");
        }
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

    private static boolean isInDoubleOrSingleQuotes(String str) {
        return str.length() > 2 && (str.charAt(0) == '"' || str.charAt(0) == '\'')
            && (str.charAt(str.length() - 1) == '"' || str.charAt(str.length() - 1) == '\'');
    }
}
