/*
 * Copyright 2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.query;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.RegexFlag;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.query.qualifier.QualifierKey;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.util.Pair;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import static com.aerospike.client.command.ParticleType.BLOB;
import static com.aerospike.client.command.ParticleType.BOOL;
import static com.aerospike.client.command.ParticleType.INTEGER;
import static com.aerospike.client.command.ParticleType.LIST;
import static com.aerospike.client.command.ParticleType.MAP;
import static com.aerospike.client.command.ParticleType.STRING;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.*;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.convertToStringListExclStart;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.getDotPathArray;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.resolveCtxList;
import static org.springframework.data.aerospike.util.FilterOperationRegexpBuilder.getContaining;
import static org.springframework.data.aerospike.util.FilterOperationRegexpBuilder.getEndsWith;
import static org.springframework.data.aerospike.util.FilterOperationRegexpBuilder.getNotContaining;
import static org.springframework.data.aerospike.util.FilterOperationRegexpBuilder.getStartsWith;
import static org.springframework.data.aerospike.util.FilterOperationRegexpBuilder.getStringEquals;
import static org.springframework.data.aerospike.util.Utils.ctxArrToString;
import static org.springframework.data.aerospike.util.Utils.getExpType;
import static org.springframework.data.aerospike.util.Utils.getExpValOrFail;

public enum FilterOperation {
    /**
     * Conjunction of two or more Qualifiers using logical AND.
     */
    AND {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            Qualifier[] qs = (Qualifier[]) qualifierMap.get(QUALIFIERS);
            Exp[] childrenExp;
            if (qs != null && qs.length > 0) {
                childrenExp = new Exp[qs.length];
            } else {
                throw new IllegalArgumentException("Expecting qualifiers not to be null with AND filter operation");
            }
            if (qs.length > 1) {
                for (int i = 0; i < qs.length; i++) {
                    childrenExp[i] = qs[i].getFilterExp();
                }
            } else {
                return qs[0].getFilterExp();
            }
            return Exp.and(childrenExp);
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null;
        }
    },
    /**
     * Conjunction of two or more Qualifiers using logical OR.
     */
    OR {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            Qualifier[] qs = (Qualifier[]) qualifierMap.get(QUALIFIERS);
            Exp[] childrenExp;
            if (qs != null && qs.length > 0) {
                childrenExp = new Exp[qs.length];
            } else {
                throw new IllegalArgumentException("Expecting qualifiers not to be null with OR filter operation");
            }
            if (qs.length > 1) {
                for (int i = 0; i < qs.length; i++) {
                    childrenExp[i] = qs[i].getFilterExp();
                }
            } else {
                return qs[0].getFilterExp();
            }
            return Exp.or(childrenExp);
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null;
        }
    },
    /**
     * For use in queries "find by <...> in a given Collection".
     */
    IN {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            // Convert IN to EQ with logical OR as there is no direct support for IN query
            return getMetadataExp(qualifierMap).orElseGet(() -> {
                Collection<?> collection = getValueAsCollectionOrFail(qualifierMap);
                Exp[] arrElementsExp = collection.stream().map(item ->
                    Qualifier.builder()
                        .setPath(getBinName(qualifierMap))
                        .setFilterOperation(FilterOperation.EQ)
                        .setValue(item)
                        .build()
                        .getFilterExp()
                ).toArray(Exp[]::new);

                return arrElementsExp.length > 1 ? Exp.or(arrElementsExp) : arrElementsExp[0];
            });
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null;
        }
    },
    /**
     * For use in queries "find by <...> not in a given Collection".
     */
    NOT_IN {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            // Convert NOT_IN to NOTEQ with logical AND as there is no direct support for NOT_IN query
            return getMetadataExp(qualifierMap).orElseGet(() -> {
                Collection<?> collection = getValueAsCollectionOrFail(qualifierMap);
                Exp[] arrElementsExp = collection.stream().map(item ->
                    Qualifier.builder()
                        .setPath(getBinName(qualifierMap))
                        .setFilterOperation(FilterOperation.NOTEQ)
                        .setValue(item)
                        .build()
                        .getFilterExp()
                ).toArray(Exp[]::new);

                return arrElementsExp.length > 1 ? Exp.and(arrElementsExp) : arrElementsExp[0];
            });
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null;
        }
    },
    /**
     * For use in queries "find by <...> equals a given object".
     */
    EQ {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            return getMetadataExp(qualifierMap).orElseGet(() -> {
                Value value = getValue(qualifierMap);
                return switch (value.getType()) {
                    case INTEGER -> Exp.eq(Exp.intBin(getBinName(qualifierMap)), Exp.val(value.toLong()));
                    case STRING -> {
                        if (ignoreCase(qualifierMap)) {
                            String equalsRegexp = getStringEquals(value.toString());
                            yield Exp.regexCompare(equalsRegexp, RegexFlag.ICASE,
                                Exp.stringBin(getBinName(qualifierMap)));
                        } else {
                            yield Exp.eq(Exp.stringBin(getBinName(qualifierMap)), Exp.val(value.toString()));
                        }
                    }
                    case BOOL -> Exp.eq(Exp.boolBin(getBinName(qualifierMap)), Exp.val((Boolean) value.getObject()));
                    case MAP -> getFilterExp(Exp.val((Map<?, ?>) value.getObject()), getBinName(qualifierMap), Exp::eq,
                        Exp::mapBin);
                    case LIST -> getFilterExp(Exp.val((List<?>) value.getObject()), getBinName(qualifierMap), Exp::eq,
                        Exp::listBin);
                    case BLOB -> Exp.eq(Exp.blobBin(getBinName(qualifierMap)), Exp.val((byte[]) value.getObject()));
                    default -> throw new IllegalArgumentException("EQ FilterExpression unsupported particle type: " +
                        value.getClass().getSimpleName());
                };
            });
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            Value value = getValue(qualifierMap);
            return switch (value.getType()) {
                // No CTX here because this FilterOperation is meant for the first level objects
                case INTEGER -> {
                    if (hasIndexName(qualifierMap)) {
                        // Having secondary index name means that this index must be used
                        yield Filter.equalByIndex(getSecondaryIndexName(qualifierMap), value.toLong());
                    }
                    yield Filter.equal(getBinName(qualifierMap), value.toLong());
                }
                case STRING -> {
                    // There is no case-insensitive string comparison filter.
                    if (ignoreCase(qualifierMap)) {
                        yield null;
                    }
                    if (hasIndexName(qualifierMap)) {
                        // Having secondary index name means that this index must be used
                        yield Filter.equalByIndex(getSecondaryIndexName(qualifierMap), value.toString());
                    }
                    yield Filter.equal(getBinName(qualifierMap), value.toString());
                }
                case BLOB -> {
                    if (!FilterOperation.getServerVersionSupport(qualifierMap).isServerVersionGtOrEq7()) {
                        yield null;
                    }
                    if (hasIndexName(qualifierMap)) {
                        // Having secondary index name means that this index must be used
                        yield Filter.equalByIndex(getSecondaryIndexName(qualifierMap), (byte[]) value.getObject());
                    }
                    yield Filter.equal(getBinName(qualifierMap), (byte[]) value.getObject());
                }
                default -> null;
            };
        }
    },
    /**
     * For use in queries "find by <...> not equal to a given object".
     */
    NOTEQ {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            return getMetadataExp(qualifierMap).orElseGet(() -> {
                Value value = getValue(qualifierMap);
                Exp ne = switch (value.getType()) {
                    // FMWK-175: Exp.ne() does not return null bins, so Exp.not(Exp.binExists()) is added
                    case INTEGER -> Exp.ne(Exp.intBin(getBinName(qualifierMap)), Exp.val(value.toLong()));
                    case STRING -> {
                        if (ignoreCase(qualifierMap)) {
                            String equalsRegexp = getStringEquals(value.toString());
                            yield Exp.not(Exp.regexCompare(equalsRegexp, RegexFlag.ICASE,
                                Exp.stringBin(getBinName(qualifierMap))));
                        } else {
                            yield Exp.ne(Exp.stringBin(getBinName(qualifierMap)), Exp.val(value.toString()));
                        }
                    }
                    case BOOL -> Exp.ne(Exp.boolBin(getBinName(qualifierMap)), Exp.val((Boolean) value.getObject()));
                    case MAP -> getFilterExp(Exp.val((Map<?, ?>) value.getObject()), getBinName(qualifierMap), Exp::ne,
                        Exp::mapBin);
                    case LIST -> getFilterExp(Exp.val((List<?>) value.getObject()), getBinName(qualifierMap), Exp::ne,
                        Exp::listBin);
                    default -> throw new IllegalArgumentException("NOTEQ FilterExpression unsupported particle type: " +
                        value.getClass().getSimpleName());
                };
                return Exp.or(Exp.not(Exp.binExists(getBinName(qualifierMap))), ne);
            });
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null; // not supported in the secondary index filter
        }
    },
    /**
     * For use in queries "find by <...> greater than a given object".
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering">Information about ordering</a>.
     */
    GT {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            return getMetadataExp(qualifierMap).orElseGet(() -> {
                Value value = getValue(qualifierMap);
                return switch (value.getType()) {
                    case INTEGER -> Exp.gt(Exp.intBin(getBinName(qualifierMap)), Exp.val(value.toLong()));
                    case STRING -> Exp.gt(Exp.stringBin(getBinName(qualifierMap)), Exp.val(value.toString()));
                    case MAP -> getFilterExp(Exp.val((Map<?, ?>) value.getObject()), getBinName(qualifierMap), Exp::gt,
                        Exp::mapBin);
                    case LIST -> getFilterExp(Exp.val((List<?>) value.getObject()), getBinName(qualifierMap), Exp::gt,
                        Exp::listBin);
                    default -> throw new IllegalArgumentException("GT FilterExpression unsupported particle type: " +
                        value.getClass().getSimpleName());
                };
            });
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            // Long.MAX_VALUE shall not be given as + 1 will cause overflow
            if (getValue(qualifierMap).getType() != INTEGER || getValue(qualifierMap).toLong() == Long.MAX_VALUE) {
                return null;
            }

            if (hasIndexName(qualifierMap)) {
                // Having secondary index name means that this index must be used
                return Filter.rangeByIndex(getSecondaryIndexName(qualifierMap), getValue(qualifierMap).toLong() + 1,
                    Long.MAX_VALUE);
            }
            return Filter.range(getBinName(qualifierMap), getValue(qualifierMap).toLong() + 1, Long.MAX_VALUE);
        }
    },
    /**
     * For use in queries "find by <...> greater than a given object or equal to it".
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering">Information about ordering</a>.
     */
    GTEQ {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            return getMetadataExp(qualifierMap).orElseGet(() -> {
                Value value = getValue(qualifierMap);
                return switch (value.getType()) {
                    case INTEGER -> Exp.ge(Exp.intBin(getBinName(qualifierMap)), Exp.val(value.toLong()));
                    case STRING -> Exp.ge(Exp.stringBin(getBinName(qualifierMap)), Exp.val(value.toString()));
                    case MAP -> getFilterExp(Exp.val((Map<?, ?>) value.getObject()), getBinName(qualifierMap), Exp::ge,
                        Exp::mapBin);
                    case LIST -> getFilterExp(Exp.val((List<?>) value.getObject()), getBinName(qualifierMap), Exp::ge,
                        Exp::listBin);
                    default -> throw new IllegalArgumentException("GTEQ FilterExpression unsupported particle type: " +
                        value.getClass().getSimpleName());
                };
            });
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            if (getValue(qualifierMap).getType() != INTEGER) {
                return null;
            }

            if (hasIndexName(qualifierMap)) {
                // Having secondary index name means that this index must be used
                return Filter.rangeByIndex(getSecondaryIndexName(qualifierMap), getValue(qualifierMap).toLong(),
                    Long.MAX_VALUE);
            }
            return Filter.range(getBinName(qualifierMap), getValue(qualifierMap).toLong(), Long.MAX_VALUE);
        }
    },
    /**
     * For use in queries "find by <...> less than a given object".
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering">Information about ordering</a>.
     */
    LT {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            return getMetadataExp(qualifierMap).orElseGet(() -> {
                Value value = getValue(qualifierMap);
                return switch (value.getType()) {
                    case INTEGER -> Exp.lt(Exp.intBin(getBinName(qualifierMap)), Exp.val(value.toLong()));
                    case STRING -> Exp.lt(Exp.stringBin(getBinName(qualifierMap)), Exp.val(value.toString()));
                    case MAP -> getFilterExp(Exp.val((Map<?, ?>) value.getObject()), getBinName(qualifierMap), Exp::lt,
                        Exp::mapBin);
                    case LIST -> getFilterExp(Exp.val((List<?>) value.getObject()), getBinName(qualifierMap), Exp::lt,
                        Exp::listBin);
                    default -> throw new IllegalArgumentException("LT FilterExpression unsupported particle type: " +
                        value.getClass().getSimpleName());
                };
            });
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            // Long.MIN_VALUE shall not be given as - 1 will cause overflow
            if (getValue(qualifierMap).getType() != INTEGER || getValue(qualifierMap).toLong() == Long.MIN_VALUE) {
                return null;
            }

            if (hasIndexName(qualifierMap)) {
                // Having secondary index name means that this index must be used
                return Filter.rangeByIndex(getSecondaryIndexName(qualifierMap), Long.MIN_VALUE,
                    getValue(qualifierMap).toLong() - 1);
            }
            return Filter.range(getBinName(qualifierMap), Long.MIN_VALUE, getValue(qualifierMap).toLong() - 1);
        }
    },
    /**
     * For use in queries "find by <...> less than a given object or equal to it".
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering">Information about ordering</a>.
     */
    LTEQ {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            return getMetadataExp(qualifierMap).orElseGet(() -> {
                Value value = getValue(qualifierMap);
                return switch (value.getType()) {
                    case INTEGER -> Exp.le(Exp.intBin(getBinName(qualifierMap)), Exp.val(value.toLong()));
                    case STRING -> Exp.le(Exp.stringBin(getBinName(qualifierMap)), Exp.val(value.toString()));
                    case MAP -> getFilterExp(Exp.val((Map<?, ?>) value.getObject()), getBinName(qualifierMap), Exp::le,
                        Exp::mapBin);
                    case LIST -> getFilterExp(Exp.val((List<?>) value.getObject()), getBinName(qualifierMap), Exp::le,
                        Exp::listBin);
                    default -> throw new IllegalArgumentException("LTEQ FilterExpression unsupported particle type: " +
                        value.getClass().getSimpleName());
                };
            });
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            if (getValue(qualifierMap).getType() != INTEGER) {
                return null;
            }

            if (hasIndexName(qualifierMap)) {
                // Having secondary index name means that this index must be used
                return Filter.rangeByIndex(getSecondaryIndexName(qualifierMap), Long.MIN_VALUE,
                    getValue(qualifierMap).toLong());
            }
            return Filter.range(getBinName(qualifierMap), Long.MIN_VALUE, getValue(qualifierMap).toLong());
        }
    },
    /**
     * For use in queries "find by <...> between two given objects".
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering">Information about ordering</a>.
     */
    BETWEEN {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            Value value = getValue(qualifierMap);
            Value value2 = getSecondValue(qualifierMap);
            return getMetadataExp(qualifierMap).orElseGet(() -> switch (value.getType()) {
                case INTEGER -> Exp.and(
                    Exp.ge(Exp.intBin(getBinName(qualifierMap)), Exp.val(value.toLong())),
                    Exp.lt(Exp.intBin(getBinName(qualifierMap)), Exp.val(value2.toLong()))
                );
                case STRING -> Exp.and(
                    Exp.ge(Exp.stringBin(getBinName(qualifierMap)), Exp.val(value.toString())),
                    Exp.lt(Exp.stringBin(getBinName(qualifierMap)), Exp.val(value2.toString()))
                );
                case MAP -> Exp.and(
                    getFilterExp(Exp.val((Map<?, ?>) value.getObject()),
                        getBinName(qualifierMap), Exp::ge, Exp::mapBin),
                    getFilterExp(Exp.val((Map<?, ?>) value2.getObject()),
                        getBinName(qualifierMap), Exp::lt, Exp::mapBin)
                );
                case LIST -> Exp.and(
                    getFilterExp(Exp.val((List<?>) value.getObject()), getBinName(qualifierMap),
                        Exp::ge, Exp::listBin),
                    getFilterExp(Exp.val((List<?>) value2.getObject()), getBinName(qualifierMap),
                        Exp::lt, Exp::listBin)
                );
                default -> throw new IllegalArgumentException("BETWEEN: unexpected value of type " + value.getClass()
                    .getSimpleName());
            });
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            if (getValue(qualifierMap).getType() != INTEGER || getSecondValue(qualifierMap).getType() != INTEGER) {
                return null;
            }

            if (hasIndexName(qualifierMap)) {
                // Having secondary index name means that this index must be used
                return Filter.rangeByIndex(getSecondaryIndexName(qualifierMap), getValue(qualifierMap).toLong(),
                    getSecondValue(qualifierMap).toLong());
            }
            return Filter.range(getBinName(qualifierMap), getValue(qualifierMap).toLong(),
                getSecondValue(qualifierMap).toLong());
        }
    },
    /**
     * For use in queries "find by <...> starts with a given String".
     */
    STARTS_WITH {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            String startWithRegexp = getStartsWith(getValue(qualifierMap).toString());
            return Exp.regexCompare(startWithRegexp, regexFlags(qualifierMap), Exp.stringBin(getBinName(qualifierMap)));
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null; // String secondary index does not support "starts with" queries
        }
    },
    /**
     * For use in queries "find by <...> ends with a given String".
     */
    ENDS_WITH {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            String endWithRegexp = getEndsWith(getValue(qualifierMap).toString());
            return Exp.regexCompare(endWithRegexp, regexFlags(qualifierMap), Exp.stringBin(getBinName(qualifierMap)));
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null; // String secondary index does not support "ends with" queries
        }
    },
    /**
     * For use in queries "find by <...> containing a given object".
     */
    CONTAINING {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            String containingRegexp = getContaining(getValue(qualifierMap).toString());
            return Exp.regexCompare(containingRegexp, regexFlags(qualifierMap),
                Exp.stringBin(getBinName(qualifierMap)));
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null; // String secondary index does not support "contains" queries
        }
    },
    /**
     * For use in queries "find by <...> not containing a given object".
     */
    NOT_CONTAINING {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            String notContainingRegexp = getNotContaining(getValue(qualifierMap).toString());
            return Exp.or(Exp.not(Exp.binExists(getBinName(qualifierMap))),
                Exp.not(Exp.regexCompare(notContainingRegexp, regexFlags(qualifierMap),
                    Exp.stringBin(getBinName(qualifierMap)))));
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null; // String secondary index does not support "contains" queries
        }
    },
    /**
     * For use in queries "find by <...> like a given String".
     */
    LIKE {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            int flags = RegexFlag.EXTENDED;
            if (ignoreCase(qualifierMap)) {
                flags = RegexFlag.EXTENDED | RegexFlag.ICASE;
            }
            if (hasId(qualifierMap)) {
                return Exp.regexCompare(getValue(qualifierMap).toString(), flags, Exp.key(Exp.Type.STRING));
            }
            return Exp.regexCompare(getValue(qualifierMap).toString(), flags, Exp.stringBin(getBinName(qualifierMap)));
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null; // not supported
        }
    },
    /**
     * For use in queries "find records where Map key <...> has value equal to a given object".
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_VAL_EQ_BY_KEY {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            return getFilterExpMapValEqOrFail(qualifierMap, Exp::eq);
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return switch (getValue(qualifierMap).getType()) {
                case STRING -> {
                    if (ignoreCase(qualifierMap)) { // there is no case-insensitive string comparison filter
                        yield null;
                    }
                    yield Filter.contains(getBinName(qualifierMap), IndexCollectionType.MAPVALUES,
                        getValue(qualifierMap).toString(), getCtxArr(qualifierMap));
                }
                case INTEGER -> Filter.range(getBinName(qualifierMap), IndexCollectionType.MAPVALUES,
                    getValue(qualifierMap).toLong(), getValue(qualifierMap).toLong(),
                    getCtxArr(qualifierMap));
                default -> null;
            };
        }
    },
    /**
     * For use in queries "find records where Map key <...> has value not equal to a given object".
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_VAL_NOTEQ_BY_KEY {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            Exp binIsNull = Exp.not(Exp.binExists(getBinName(qualifierMap)));
            String errMsg = "MAP_VAL_NOTEQ_BY_KEY FilterExpression unsupported type: got " +
                getKey(qualifierMap).getClass().getSimpleName();
            Exp keyIsNull = mapKeysCountComparedToZero(qualifierMap, Exp::eq, getKey(qualifierMap), errMsg);
            return Exp.or(binIsNull, keyIsNull, getFilterExpMapValNotEqOrFail(qualifierMap, Exp::ne));
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null; // not supported
        }
    },
    /**
     * For use in queries "find records where Map key <...> has value equal to one of the objects in a given
     * Collection".
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_VAL_IN_BY_KEY {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            // Convert IN to EQ with logical OR as there is no direct support for IN query
            Collection<?> collection = getValueAsCollectionOrFail(qualifierMap);
            String pathPart = getKey(qualifierMap).toString();
            if (getCtxArr(qualifierMap) != null) pathPart = getCtxArrAsString(qualifierMap) + pathPart;
            String path = String.join(".", getBinName(qualifierMap), pathPart);

            Exp[] arrElementsExp = collection.stream().map(item ->
                Qualifier.builder()
                    .setFilterOperation(FilterOperation.MAP_VAL_EQ_BY_KEY)
                    .setPath(path)
                    .setValue(item)
                    .build()
                    .getFilterExp()
            ).toArray(Exp[]::new);

            return Exp.or(arrElementsExp);
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null;
        }
    },
    /**
     * For use in queries "find records where Map key <...> has value equal to neither of the objects in a given
     * Collection".
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_VAL_NOT_IN_BY_KEY {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            // Convert NOT_IN to NOTEQ with logical AND as there is no direct support for NOT_IN query
            Collection<?> collection = getValueAsCollectionOrFail(qualifierMap);
            String pathPart = getKey(qualifierMap).toString();
            if (getCtxArr(qualifierMap) != null) pathPart = getCtxArrAsString(qualifierMap) + pathPart;
            String path = String.join(".", getBinName(qualifierMap), pathPart);
            Exp[] arrElementsExp = collection.stream().map(item ->
                Qualifier.builder()
                    .setFilterOperation(FilterOperation.MAP_VAL_NOTEQ_BY_KEY)
                    .setPath(path)
                    .setValue(item)
                    .build()
                    .getFilterExp()
            ).toArray(Exp[]::new);

            return Exp.and(arrElementsExp);
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null;
        }
    },
    /**
     * For use in queries "find records where Map key <...> has value greater than a given object".
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering">Information about ordering</a>.
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_VAL_GT_BY_KEY {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            return getFilterExpMapValOrFail(qualifierMap, Exp::gt, "MAP_VAL_GT_BY_KEY");
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            // Long.MAX_VALUE shall not be given as + 1 will cause overflow
            if (getKey(qualifierMap).getType() != INTEGER || getKey(qualifierMap).toLong() == Long.MAX_VALUE) {
                return null;
            }

            if (hasIndexName(qualifierMap)) {
                // Having secondary index name means that this index must be used
                return Filter.rangeByIndex(getSecondaryIndexName(qualifierMap), IndexCollectionType.MAPVALUES,
                    getKey(qualifierMap).toLong() + 1, Long.MAX_VALUE);
            }
            return Filter.range(getBinName(qualifierMap), IndexCollectionType.MAPVALUES,
                getKey(qualifierMap).toLong() + 1,
                Long.MAX_VALUE, getCtxArr(qualifierMap));
        }
    },
    /**
     * For use in queries "find records where Map key <...> has value greater than a given object or equal to it".
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering">Information about ordering</a>.
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_VAL_GTEQ_BY_KEY {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            return getFilterExpMapValOrFail(qualifierMap, Exp::ge, "MAP_VAL_GTEQ_BY_KEY");
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            if (getKey(qualifierMap).getType() != INTEGER) {
                return null;
            }
            if (hasIndexName(qualifierMap)) {
                // Having secondary index name means that this index must be used
                return Filter.rangeByIndex(getSecondaryIndexName(qualifierMap), IndexCollectionType.MAPVALUES,
                    getKey(qualifierMap).toLong(), Long.MAX_VALUE);
            }
            return Filter.range(getBinName(qualifierMap), IndexCollectionType.MAPVALUES,
                getKey(qualifierMap).toLong(), Long.MAX_VALUE, getCtxArr(qualifierMap));
        }
    },
    /**
     * For use in queries "find records where Map key <...> has value less than a given object".
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering">Information about ordering</a>.
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_VAL_LT_BY_KEY {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            return getFilterExpMapValOrFail(qualifierMap, Exp::lt, "MAP_VAL_LT_BY_KEY");
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            // Long.MIN_VALUE shall not be given as - 1 will cause overflow
            if (getKey(qualifierMap).getType() != INTEGER || getKey(qualifierMap).toLong() == Long.MIN_VALUE) {
                return null;
            }
            if (hasIndexName(qualifierMap)) {
                // Having secondary index name means that this index must be used
                return Filter.rangeByIndex(getSecondaryIndexName(qualifierMap), IndexCollectionType.MAPVALUES, Long.MIN_VALUE,
                    getKey(qualifierMap).toLong() - 1);
            }
            return Filter.range(getBinName(qualifierMap), IndexCollectionType.MAPVALUES, Long.MIN_VALUE,
                getKey(qualifierMap).toLong() - 1, getCtxArr(qualifierMap));
        }
    },
    /**
     * For use in queries "find records where Map key <...> has value less than a given object or equal to it".
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering">Information about ordering</a>.
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_VAL_LTEQ_BY_KEY {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            return getFilterExpMapValOrFail(qualifierMap, Exp::le, "MAP_VAL_LTEQ_BY_KEY");
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            if (getValue(qualifierMap).getType() != INTEGER) {
                return null;
            }
            if (hasIndexName(qualifierMap)) {
                // Having secondary index name means that this index must be used
                return Filter.rangeByIndex(getSecondaryIndexName(qualifierMap), IndexCollectionType.MAPVALUES, Long.MIN_VALUE,
                    getValue(qualifierMap).toLong());
            }
            return Filter.range(getBinName(qualifierMap), IndexCollectionType.MAPVALUES, Long.MIN_VALUE,
                getValue(qualifierMap).toLong(), getCtxArr(qualifierMap));
        }
    },
    /**
     * For use in queries "find records where Map key <...> has value between two given objects".
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering">Information about ordering</a>.
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_VAL_BETWEEN_BY_KEY {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            CTX[] ctxArr = getCtxArr(qualifierMap);
            Exp lowerLimit;
            Exp upperLimit;
            Exp.Type type;
            switch (getValue(qualifierMap).getType()) {
                case INTEGER -> {
                    lowerLimit = Exp.val(getValue(qualifierMap).toLong());
                    upperLimit = Exp.val(getSecondValue(qualifierMap).toLong());
                    type = Exp.Type.INT;
                }
                case STRING -> {
                    lowerLimit = Exp.val(getValue(qualifierMap).toString());
                    upperLimit = Exp.val(getSecondValue(qualifierMap).toString());
                    type = Exp.Type.STRING;
                }
                case LIST -> {
                    lowerLimit = Exp.val((List<?>) getValue(qualifierMap).getObject());
                    upperLimit = Exp.val((List<?>) getSecondValue(qualifierMap).getObject());
                    type = Exp.Type.LIST;
                }
                case MAP -> {
                    lowerLimit = Exp.val((Map<?, ?>) getValue(qualifierMap).getObject());
                    upperLimit = Exp.val((Map<?, ?>) getSecondValue(qualifierMap).getObject());
                    type = Exp.Type.MAP;
                }
                default -> throw new IllegalArgumentException(
                    "MAP_VAL_BETWEEN_BY_KEY FilterExpression unsupported type: got " +
                        getValue(qualifierMap).getClass().getSimpleName());
            }

            return mapValBetweenByKey(qualifierMap, ctxArr, type, lowerLimit, upperLimit);
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            if (getValue(qualifierMap).getType() != INTEGER || getSecondValue(qualifierMap).getType() != INTEGER) {
                return null;
            }
            if (hasIndexName(qualifierMap)) {
                // Having secondary index name means that this index must be used
                return Filter.rangeByIndex(getSecondaryIndexName(qualifierMap), IndexCollectionType.MAPVALUES,
                    getValue(qualifierMap).toLong(), getSecondValue(qualifierMap).toLong());
            }
            return Filter.range(getBinName(qualifierMap), IndexCollectionType.MAPVALUES,
                getValue(qualifierMap).toLong(), getSecondValue(qualifierMap).toLong(),
                getCtxArr(qualifierMap));
        }
    },
    /**
     * For use in queries "find records where Map key <...> has value that starts with a given String".
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_VAL_STARTS_WITH_BY_KEY {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            String startWithRegexp = getStartsWith(getValue(qualifierMap).toString());

            return Exp.regexCompare(startWithRegexp, regexFlags(qualifierMap),
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getKey(qualifierMap).toString()),
                    Exp.mapBin(getBinName(qualifierMap)))
            );
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null; // String secondary index does not support "starts with" queries
        }
    },
    /**
     * For use in queries "find records where Map key <...> has value that is like a given String".
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_VAL_LIKE_BY_KEY {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            int flags = RegexFlag.EXTENDED;
            if (ignoreCase(qualifierMap)) {
                flags = RegexFlag.EXTENDED | RegexFlag.ICASE;
            }
            return Exp.regexCompare(getValue(qualifierMap).toString(), flags,
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getKey(qualifierMap).toString()),
                    Exp.mapBin(getBinName(qualifierMap)))
            );
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null; // not supported
        }
    },
    /**
     * For use in queries "find records where Map key <...> has value that ends with a given String".
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_VAL_ENDS_WITH_BY_KEY {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            String endWithRegexp = getEndsWith(getKey(qualifierMap).toString());

            return Exp.regexCompare(endWithRegexp, regexFlags(qualifierMap),
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getValue(qualifierMap).toString()),
                    Exp.mapBin(getBinName(qualifierMap)))
            );
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null; // String secondary index does not support "ends with" queries
        }
    },
    /**
     * For use in queries "find records where Map key <...> has value that contains a given object".
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_VAL_CONTAINING_BY_KEY {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            Integer nestedType = FilterOperation.getNestedType(qualifierMap);
            if (nestedType == null) throw new IllegalStateException("Expecting valid nestedType, got null");
            switch (nestedType) {
                case STRING -> {
                    // Out of simple properties only a String is validated for CONTAINING
                    String containingRegexp = getContaining(getValue(qualifierMap).toString());
                    Exp nestedString = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                        Exp.val(getKey(qualifierMap).toString()), Exp.mapBin(getBinName(qualifierMap)));
                    return Exp.regexCompare(containingRegexp, regexFlags(qualifierMap), nestedString);
                }
                case LIST -> {
                    Exp value = getExpOrFail(getValue(qualifierMap), "MAP_VAL_CONTAINING_BY_KEY");
                    Exp key = getExpOrFail(getKey(qualifierMap), "MAP_VAL_CONTAINING_BY_KEY");

                    // Map value is a List
                    Exp nestedList = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.LIST, key,
                        Exp.mapBin(getBinName(qualifierMap)));
                    // Check whether List contains the value
                    return Exp.gt(
                        ListExp.getByValue(ListReturnType.COUNT, value, nestedList),
                        Exp.val(0));
                }
                case MAP -> {
                    Value val = getValue(qualifierMap);
                    Exp value = getExpOrFail(val, "MAP_VAL_CONTAINING_BY_KEY");
                    Exp key = getExpOrFail(getKey(qualifierMap), "MAP_VAL_CONTAINING_BY_KEY");
                    Exp secondKey = getExpOrFail(getNestedKey(qualifierMap), "MAP_VAL_CONTAINING_BY_KEY");

                    Exp nestedMap = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.MAP, key,
                        Exp.mapBin(getBinName(qualifierMap)));
                    return Exp.eq(
                        MapExp.getByKey(MapReturnType.VALUE, getExpType(val), secondKey, nestedMap),
                        value);
                }
                default -> throw new UnsupportedOperationException("Unsupported nested type: " + nestedType);
            }
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null; // String secondary index does not support "contains" queries
        }
    },
    /**
     * For use in queries "find records where Map key <...> does not have value that contains a given object".
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_VAL_NOT_CONTAINING_BY_KEY {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            Integer nestedType = getNestedType(qualifierMap);
            Exp mapBinDoesNotExist = Exp.not(Exp.binExists(getBinName(qualifierMap)));
            Exp key = getExpOrFail(getKey(qualifierMap), "MAP_VAL_NOT_CONTAINING_BY_KEY");
            Exp mapNotContainingKey = Exp.eq(
                MapExp.getByKey(MapReturnType.COUNT, Exp.Type.INT, key, Exp.mapBin(getBinName(qualifierMap))),
                Exp.val(0));

            if (nestedType == null) throw new IllegalStateException("Expecting valid nestedType, got null");
            switch (nestedType) {
                case STRING -> {
                    // Out of simple properties only a String is validated for NOT_CONTAINING
                    String containingRegexp = getContaining(getValue(qualifierMap).toString());
                    Exp nestedString = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                        Exp.val(getKey(qualifierMap).toString()),
                        Exp.mapBin(getBinName(qualifierMap)));
                    return Exp.or(mapBinDoesNotExist, mapNotContainingKey,
                        Exp.not(Exp.regexCompare(containingRegexp, regexFlags(qualifierMap), nestedString)));
                }
                case LIST -> {
                    Exp value = getExpOrFail(getValue(qualifierMap), "MAP_VAL_NOT_CONTAINING_BY_KEY");

                    Exp nestedList = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.LIST, key,
                        Exp.mapBin(getBinName(qualifierMap)));
                    Exp nestedListNotContainingValue = Exp.eq(ListExp.getByValue(ListReturnType.COUNT, value,
                        nestedList), Exp.val(0));
                    return Exp.or(mapBinDoesNotExist, mapNotContainingKey, nestedListNotContainingValue);
                }
                case MAP -> {
                    Value val = getValue(qualifierMap);
                    Exp value = getExpOrFail(val, "MAP_VAL_NOT_CONTAINING_BY_KEY");
                    Exp secondKey = getExpOrFail(getNestedKey(qualifierMap), "MAP_VAL_NOT_CONTAINING_BY_KEY");

                    Exp nestedMap = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.MAP, key,
                        Exp.mapBin(getBinName(qualifierMap)));
                    Exp nestedMapNotContainingValueByKey = Exp.ne(
                        MapExp.getByKey(MapReturnType.VALUE, getExpType(val), secondKey, nestedMap),
                        value);
                    return Exp.or(mapBinDoesNotExist, mapNotContainingKey, nestedMapNotContainingValueByKey);
                }
                default -> throw new UnsupportedOperationException("Unsupported value type: " + nestedType);
            }
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null; // String secondary index does not support "contains" queries
        }
    },
    /**
     * For use in queries "find records where Map key <...> has existing value".
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_VAL_EXISTS_BY_KEY {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            return mapKeysContain(qualifierMap);
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null;
        }
    },
    /**
     * For use in queries "find records where Map key <...> does not have existing value".
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_VAL_NOT_EXISTS_BY_KEY {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            return mapKeysNotContain(qualifierMap);
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null;
        }
    },
    /**
     * For use in queries "find records where Map key <...> has value that is not null (i.e. exists)".
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_VAL_IS_NOT_NULL_BY_KEY {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            String[] dotPathArray = getDotPathArray(getDotPath(qualifierMap));
            if (dotPathArray != null && dotPathArray.length > 1) {
                // in case it is a field of an object set to null the key does not get added to a Map,
                // so it is enough to look for Maps with the given key
                return mapKeysContain(qualifierMap);
            } else {
                // currently querying for a specific Map key with not null value is not supported,
                // it is recommended to use querying for an existing key and then filtering key:!=null
                return getMapValEqOrFail(qualifierMap, Exp::eq, "MAP_VAL_IS_NOT_NULL_BY_KEY");
            }
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null;
        }
    },
    /**
     * For use in queries "find records where Map key <...> has value that is null (i.e. does not exist)".
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_VAL_IS_NULL_BY_KEY {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            String[] dotPathArray = getDotPathArray(getDotPath(qualifierMap));
            if (dotPathArray != null && dotPathArray.length > 1) {
                // in case it is a field of an object set to null the key does not get added to a Map,
                // so it is enough to look for Maps without the given key
                return mapKeysNotContain(qualifierMap);
            } else {
                // currently querying for a specific Map key with explicit null value is not supported,
                // it is recommended to use querying for an existing key and then filtering key:null
                return getMapValEqOrFail(qualifierMap, Exp::eq, "MAP_VAL_IS_NULL_BY_KEY");
            }
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null;
        }
    },
    /**
     * For use in queries "find records where keys of Map <...> contain a given object".
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_KEYS_CONTAIN {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            return mapKeysContain(qualifierMap);
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return cdtContains(IndexCollectionType.MAPKEYS, qualifierMap, hasIndexName(qualifierMap));
        }
    },
    /**
     * For use in queries "find records where keys of Map <...> do not contain a given object".
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_KEYS_NOT_CONTAIN {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            return mapKeysNotContain(qualifierMap);
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null; // currently not supported
        }
    },
    /**
     * For use in queries "find records where values of Map <...> contain a given object".
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_VALUES_CONTAIN {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            return mapValuesContain(qualifierMap);
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return cdtContains(IndexCollectionType.MAPVALUES, qualifierMap, hasIndexName(qualifierMap));
        }
    },
    /**
     * For use in queries "find records where values of Map <...> do not contain a given object".
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_VALUES_NOT_CONTAIN {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            return mapValuesNotContain(qualifierMap);
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null; // currently not supported
        }
    },
    /**
     * For use in queries "find records where keys of Map <...> are between two given objects".
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering">Information about ordering</a>.
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_KEYS_BETWEEN {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            Pair<Exp, Exp> twoValues = switch (getValue(qualifierMap).getType()) {
                case INTEGER ->
                    Pair.of(Exp.val(getValue(qualifierMap).toLong()), Exp.val(getSecondValue(qualifierMap).toLong()));
                case STRING -> Pair.of(Exp.val(getValue(qualifierMap).toString()),
                    Exp.val(getSecondValue(qualifierMap).toString()));
                case LIST -> Pair.of(Exp.val((List<?>) getValue(qualifierMap).getObject()),
                    Exp.val((List<?>) getSecondValue(qualifierMap).getObject()));
                case MAP -> Pair.of(Exp.val((Map<?, ?>) getValue(qualifierMap).getObject()),
                    Exp.val((Map<?, ?>) getSecondValue(qualifierMap).getObject()));
                default -> throw new UnsupportedOperationException(
                    "MAP_KEYS_BETWEEN FilterExpression unsupported type: got "
                        + getValue(qualifierMap).getClass().getSimpleName());
            };

            return Exp.gt(
                MapExp.getByKeyRange(MapReturnType.COUNT, twoValues.getFirst(), twoValues.getSecond(),
                    Exp.mapBin(getBinName(qualifierMap))),
                Exp.val(0));
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return collectionRange(IndexCollectionType.MAPKEYS, qualifierMap, hasIndexName(qualifierMap));
        }
    },
    /**
     * For use in queries "find records where values of Map <...> are between two given objects".
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering">Information about ordering</a>.
     * <br>
     * Map can be a whole bin or a part of it (nested inside one or more Lists or other Maps).
     * <br>
     * Maps in Aerospike DB represent not only Java Maps, but also POJOs.
     */
    MAP_VAL_BETWEEN {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            Pair<Exp, Exp> twoValues = switch (getValue(qualifierMap).getType()) {
                case INTEGER ->
                    Pair.of(Exp.val(getValue(qualifierMap).toLong()), Exp.val(getSecondValue(qualifierMap).toLong()));
                case STRING -> Pair.of(Exp.val(getValue(qualifierMap).toString()),
                    Exp.val(getSecondValue(qualifierMap).toString()));
                case LIST -> Pair.of(Exp.val((List<?>) getValue(qualifierMap).getObject()),
                    Exp.val((List<?>) getSecondValue(qualifierMap).getObject()));
                case MAP -> Pair.of(Exp.val((Map<?, ?>) getValue(qualifierMap).getObject()),
                    Exp.val((Map<?, ?>) getSecondValue(qualifierMap).getObject()));
                default -> throw new UnsupportedOperationException(
                    "MAP_VAL_BETWEEN FilterExpression unsupported type: got "
                        + getValue(qualifierMap).getClass().getSimpleName());
            };

            return Exp.gt(
                MapExp.getByValueRange(MapReturnType.COUNT, twoValues.getFirst(), twoValues.getSecond(),
                    Exp.mapBin(getBinName(qualifierMap))),
                Exp.val(0));
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            if (getValue(qualifierMap).getType() != INTEGER || getSecondValue(qualifierMap).getType() != INTEGER) {
                return null;
            }
            return collectionRange(IndexCollectionType.MAPVALUES, qualifierMap, hasIndexName(qualifierMap));
        }
    },
    /**
     * For use in queries "find by Geo <...> within a given GeoJSON".
     */
    GEO_WITHIN {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            return Exp.geoCompare(Exp.geoBin(getBinName(qualifierMap)), Exp.geo(getValue(qualifierMap).toString()));
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return geoWithinRadius(IndexCollectionType.DEFAULT, qualifierMap, hasIndexName(qualifierMap));
        }
    },
    /**
     * For use in queries "find by Collection <...> containing a given object". The Collection can be a whole bin or
     * part of it (nested inside one or more Lists or Maps).
     */
    COLLECTION_VAL_CONTAINING {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            Value val = getValue(qualifierMap);
            // boolean values are read as BoolIntValue (INTEGER ParticleType) if Value.UseBoolBin == false
            // converting to BooleanValue to process correctly
            if (val instanceof Value.BoolIntValue) {
                qualifierMap.put(VALUE, new Value.BooleanValue((Boolean) (getValue(qualifierMap).getObject())));
            }
            String errMsg = "COLLECTION_VAL_CONTAINING FilterExpression unsupported type: got " +
                val.getClass().getSimpleName();
            Exp value = getExpValOrFail(val, errMsg);
            return Exp.gt(
                ListExp.getByValue(ListReturnType.COUNT, value, Exp.listBin(getBinName(qualifierMap))),
                Exp.val(0));
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            if (getValue(qualifierMap).getType() != STRING && getValue(qualifierMap).getType() != INTEGER) {
                return null;
            }

            return cdtContains(IndexCollectionType.LIST, qualifierMap, hasIndexName(qualifierMap));
        }
    },
    /**
     * For use in queries "find by Collection <...> not containing a given object". The Collection can be a whole bin or
     * part of it (nested inside one or more Lists or Maps).
     */
    COLLECTION_VAL_NOT_CONTAINING {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            Value val = getValue(qualifierMap);
            // boolean values are read as BoolIntValue (INTEGER ParticleType) if Value.UseBoolBin == false
            // so converting to BooleanValue to process correctly
            if (val instanceof Value.BoolIntValue) {
                qualifierMap.put(VALUE, new Value.BooleanValue((Boolean) (getKey(qualifierMap).getObject())));
            }
            String errMsg = "COLLECTION_VAL_NOT_CONTAINING FilterExpression unsupported type: got " +
                val.getClass().getSimpleName();
            Exp value = getExpValOrFail(val, errMsg);

            Exp binIsNull = Exp.not(Exp.binExists(getBinName(qualifierMap)));
            Exp listNotContaining = Exp.eq(
                ListExp.getByValue(ListReturnType.COUNT, value, Exp.listBin(getBinName(qualifierMap))),
                Exp.val(0));
            return Exp.or(binIsNull, listNotContaining);
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null; // currently not supported
        }
    },

    /**
     * For use in queries "find records where Collection <...> contains values greater than a given object".
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering">Information about ordering</a>.
     * <br>
     * The Collection can be a whole bin or a part of it (nested inside one or more Lists or Maps).
     */
    COLLECTION_VAL_GT {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            if (getValue(qualifierMap).getType() == INTEGER) {
                if (getValue(qualifierMap).toLong() == Long.MAX_VALUE) {
                    throw new IllegalArgumentException(
                        "COLLECTION_VAL_GT FilterExpression unsupported value: expected [Long.MIN_VALUE.." +
                            "Long.MAX_VALUE-1]");
                }

                return Exp.gt(
                    ListExp.getByValueRange(ListReturnType.COUNT, Exp.val(getValue(qualifierMap).toLong() + 1L),
                        null, Exp.listBin(getBinName(qualifierMap))),
                    Exp.val(0)
                );
            } else {
                Exp value = switch (getValue(qualifierMap).getType()) {
                    case STRING -> Exp.val(getValue(qualifierMap).toString());
                    case LIST -> Exp.val((List<?>) getValue(qualifierMap).getObject());
                    case MAP -> Exp.val((Map<?, ?>) getValue(qualifierMap).getObject());
                    default -> throw new UnsupportedOperationException(
                        "COLLECTION_VAL_GT FilterExpression unsupported type: got "
                            + getValue(qualifierMap).getClass().getSimpleName());
                };

                Exp rangeIncludingValue = ListExp.getByValueRange(ListReturnType.COUNT, value, null,
                    Exp.listBin(getBinName(qualifierMap)));
                Exp valueOnly = ListExp.getByValue(ListReturnType.COUNT, value, Exp.listBin(getBinName(qualifierMap)));
                return Exp.gt(Exp.sub(rangeIncludingValue, valueOnly), Exp.val(0));
            }
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            // Long.MAX_VALUE shall not be given as + 1 will cause overflow
            if (getValue(qualifierMap).getType() != INTEGER || getValue(qualifierMap).toLong() == Long.MAX_VALUE) {
                return null;
            }
            if (hasIndexName(qualifierMap)) {
                // Having secondary index name means that this index must be used
                return Filter.rangeByIndex(getSecondaryIndexName(qualifierMap), IndexCollectionType.LIST,
                    getValue(qualifierMap).toLong() + 1, Long.MAX_VALUE);
            }
            return Filter.range(getBinName(qualifierMap), IndexCollectionType.LIST,
                getValue(qualifierMap).toLong() + 1, Long.MAX_VALUE);
        }
    },
    /**
     * For use in queries "find records where Collection <...> contains values greater than a given object or equal to
     * it".
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering">Information about ordering</a>.
     * <br>
     * The Collection can be a whole bin or a part of it (nested inside one or more Lists or Maps).
     */
    COLLECTION_VAL_GTEQ {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            Exp value = switch (getValue(qualifierMap).getType()) {
                case INTEGER -> Exp.val(getValue(qualifierMap).toLong());
                case STRING -> Exp.val(getValue(qualifierMap).toString());
                case LIST -> Exp.val((List<?>) getValue(qualifierMap).getObject());
                case MAP -> Exp.val((Map<?, ?>) getValue(qualifierMap).getObject());
                default -> throw new UnsupportedOperationException(
                    "COLLECTION_VAL_GTEQ FilterExpression unsupported type: got "
                        + getValue(qualifierMap).getClass().getSimpleName());
            };

            return Exp.gt(
                ListExp.getByValueRange(ListReturnType.COUNT, value, null, Exp.listBin(getBinName(qualifierMap))),
                Exp.val(0));
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            if (getValue(qualifierMap).getType() != INTEGER) {
                return null;
            }
            if (hasIndexName(qualifierMap)) {
                // Having secondary index name means that this index must be used
                return Filter.rangeByIndex(getSecondaryIndexName(qualifierMap), IndexCollectionType.LIST,
                    getValue(qualifierMap).toLong(), Long.MAX_VALUE);
            }
            return Filter.range(getBinName(qualifierMap), IndexCollectionType.LIST, getValue(qualifierMap).toLong(),
                Long.MAX_VALUE);
        }
    },
    /**
     * For use in queries "find records where Collection <...> contains values less than a given object".
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering">Information about ordering</a>.
     * <br>
     * The Collection can be a whole bin or a part of it (nested inside one or more Lists or Maps).
     */
    COLLECTION_VAL_LT {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            Exp value = switch (getValue(qualifierMap).getType()) {
                case INTEGER -> {
                    if (getValue(qualifierMap).toLong() == Long.MIN_VALUE) {
                        throw new UnsupportedOperationException(
                            "COLLECTION_VAL_LT FilterExpression unsupported value: expected [Long.MIN_VALUE+1.." +
                                "Long.MAX_VALUE]");
                    }

                    yield Exp.val(getValue(qualifierMap).toLong());
                }
                case STRING -> Exp.val(getValue(qualifierMap).toString());
                case LIST -> Exp.val((List<?>) getValue(qualifierMap).getObject());
                case MAP -> Exp.val((Map<?, ?>) getValue(qualifierMap).getObject());
                default -> throw new UnsupportedOperationException(
                    "COLLECTION_VAL_GTEQ FilterExpression unsupported type: got "
                        + getValue(qualifierMap).getClass().getSimpleName());
            };

            return Exp.gt(
                ListExp.getByValueRange(ListReturnType.COUNT, null, value, Exp.listBin(getBinName(qualifierMap))),
                Exp.val(0));
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            // Long.MIN_VALUE shall not be given as - 1 will cause overflow
            if (getValue(qualifierMap).getType() != INTEGER || getValue(qualifierMap).toLong() == Long.MIN_VALUE) {
                return null;
            }
            if (hasIndexName(qualifierMap)) {
                // Having secondary index name means that this index must be used
                return Filter.rangeByIndex(getSecondaryIndexName(qualifierMap), IndexCollectionType.LIST, Long.MIN_VALUE,
                    getValue(qualifierMap).toLong() - 1);
            }
            return Filter.range(getBinName(qualifierMap), IndexCollectionType.LIST, Long.MIN_VALUE,
                getValue(qualifierMap).toLong() - 1);
        }
    },
    /**
     * For use in queries "find records where Collection <...> contains values less than a given object or equal to
     * it".
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering">Information about ordering</a>.
     * <br>
     * The Collection can be a whole bin or a part of it (nested inside one or more Lists or Maps).
     */
    COLLECTION_VAL_LTEQ {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            if (getValue(qualifierMap).getType() == INTEGER) {
                Exp upperLimit;
                if (getValue(qualifierMap).toLong() == Long.MAX_VALUE) {
                    upperLimit = Exp.inf();
                } else {
                    upperLimit = Exp.val(getValue(qualifierMap).toLong() + 1L);
                }

                return Exp.gt(
                    ListExp.getByValueRange(ListReturnType.COUNT, Exp.val(Long.MIN_VALUE),
                        upperLimit, Exp.listBin(getBinName(qualifierMap))),
                    Exp.val(0));
            } else {
                Exp value = switch (getValue(qualifierMap).getType()) {
                    case STRING -> Exp.val(getValue(qualifierMap).toString());
                    case LIST -> Exp.val((List<?>) getValue(qualifierMap).getObject());
                    case MAP -> Exp.val((Map<?, ?>) getValue(qualifierMap).getObject());
                    default -> throw new UnsupportedOperationException(
                        "COLLECTION_VAL_LTEQ FilterExpression unsupported type: got " +
                            getValue(qualifierMap).getClass().getSimpleName());
                };

                Exp rangeIncludingValue = ListExp.getByValueRange(ListReturnType.COUNT, null, value,
                    Exp.listBin(getBinName(qualifierMap)));
                Exp valueOnly = ListExp.getByValue(ListReturnType.COUNT, value, Exp.listBin(getBinName(qualifierMap)));
                return Exp.gt(Exp.add(rangeIncludingValue, valueOnly), Exp.val(0));
            }
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            if (getValue(qualifierMap).getType() != INTEGER) {
                return null;
            }
            if (hasIndexName(qualifierMap)) {
                // Having secondary index name means that this index must be used
                return Filter.rangeByIndex(getSecondaryIndexName(qualifierMap), IndexCollectionType.LIST, Long.MIN_VALUE,
                    getValue(qualifierMap).toLong());
            }
            return Filter.range(getBinName(qualifierMap), IndexCollectionType.LIST, Long.MIN_VALUE,
                getValue(qualifierMap).toLong());
        }
    },
    /**
     * For use in queries "find records where Collection <...> contains values between two given objects".
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering">Information about ordering</a>.
     * <br>
     * The Collection can be a whole bin or a part of it (nested inside one or more Lists or Maps).
     */
    COLLECTION_VAL_BETWEEN {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            Pair<Exp, Exp> twoValues = switch (getValue(qualifierMap).getType()) {
                case INTEGER ->
                    Pair.of(Exp.val(getValue(qualifierMap).toLong()), Exp.val(getSecondValue(qualifierMap).toLong()));
                case STRING -> Pair.of(Exp.val(getValue(qualifierMap).toString()),
                    Exp.val(getSecondValue(qualifierMap).toString()));
                case LIST -> Pair.of(Exp.val((List<?>) getValue(qualifierMap).getObject()),
                    Exp.val((List<?>) getSecondValue(qualifierMap).getObject()));
                case MAP -> Pair.of(Exp.val((Map<?, ?>) getValue(qualifierMap).getObject()),
                    Exp.val((Map<?, ?>) getSecondValue(qualifierMap).getObject()));
                default -> throw new UnsupportedOperationException(
                    "COLLECTION_VAL_BETWEEN FilterExpression unsupported type: got "
                        + getValue(qualifierMap).getClass().getSimpleName());
            };

            return Exp.gt(
                ListExp.getByValueRange(ListReturnType.COUNT, twoValues.getFirst(), twoValues.getSecond(),
                    Exp.listBin(getBinName(qualifierMap))),
                Exp.val(0));
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            if (getValue(qualifierMap).getType() != INTEGER || getSecondValue(qualifierMap).getType() != INTEGER) {
                return null;
            }
            return collectionRange(IndexCollectionType.LIST, qualifierMap, hasIndexName(qualifierMap)); // both limits are inclusive
        }
    },
    /**
     * For use in queries "find records where <...> is not null (i.e. exists)".
     */
    IS_NOT_NULL {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            return Exp.binExists(getBinName(qualifierMap));
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null;
        }
    },
    /**
     * For use in queries "find records where <...> is null (i.e. does not exist)".
     */
    IS_NULL {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            // with value set to null a bin becomes non-existing
            return Exp.not(Exp.binExists(getBinName(qualifierMap)));
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null;
        }
    }, NOT_NULL {
        @Override
        public Exp filterExp(Map<QualifierKey, Object> qualifierMap) {
            return Exp.binExists(getBinName(qualifierMap)); // if a bin exists its value is not null
        }

        @Override
        public Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap) {
            return null;
        }
    };

    private static boolean hasIndexName(Map<QualifierKey, Object> qualifierMap) {
        return hasServerVersionSupport(qualifierMap)
            && getServerVersionSupport(qualifierMap).isServerVersionGtOrEq8_1()
            && hasSecondaryIndexName(qualifierMap);
    }

    /**
     * FilterOperations that require both sIndexFilter and FilterExpression
     */
    protected static final List<FilterOperation> dualFilterOperations = Arrays.asList(
        MAP_VAL_EQ_BY_KEY, MAP_VAL_GT_BY_KEY, MAP_VAL_GTEQ_BY_KEY, MAP_VAL_LT_BY_KEY, MAP_VAL_LTEQ_BY_KEY,
        MAP_VAL_BETWEEN_BY_KEY, MAP_KEYS_BETWEEN, MAP_VAL_BETWEEN
    );

    @SuppressWarnings("unchecked")
    private static Exp processMetadataFieldInOrNot(Map<QualifierKey, Object> qualifierMap, boolean notIn) {
        FilterOperation filterOperation = notIn ? NOTEQ : EQ;
        Object obj = getValue(qualifierMap).getObject();

        Collection<Long> listOfLongs;
        try {
            listOfLongs = (Collection<Long>) obj; // previously validated
        } catch (Exception e) {
            String operation = notIn ? "NOT_IN" : "IN";
            throw new IllegalStateException("FilterOperation." + operation + " metadata query: expecting value as " +
                "a Collection<Long>");
        }
        Exp[] listElementsExp = listOfLongs.stream().map(item ->
            Qualifier.metadataBuilder()
                .setMetadataField(getMetadataField(qualifierMap))
                .setFilterOperation(filterOperation)
                .setValue(item)
                .build()
                .getFilterExp()
        ).toArray(Exp[]::new);

        return notIn ? Exp.and(listElementsExp) : Exp.or(listElementsExp);
    }

    private static Exp processMetadataFieldIn(Map<QualifierKey, Object> qualifierMap) {
        return processMetadataFieldInOrNot(qualifierMap, false);
    }

    private static Exp processMetadataFieldNotIn(Map<QualifierKey, Object> qualifierMap) {
        return processMetadataFieldInOrNot(qualifierMap, true);
    }

    private static Exp getExpOrFail(Value value, String filterOpName) {
        String errMsg = String.format("%s FilterExpression unsupported value type: got %s", filterOpName,
            value.getClass().getSimpleName());
        return getExpValOrFail(value, errMsg);
    }

    private static Collection<?> getValueAsCollectionOrFail(Map<QualifierKey, Object> qualifierMap) {
        Value value = getValue(qualifierMap);
        String errMsg = "FilterOperation.IN expects argument with type Collection, instead got: " +
            value.getObject().getClass().getSimpleName();
        if (value.getType() != LIST || !(value.getObject() instanceof Collection<?>)) {
            throw new IllegalArgumentException(errMsg);
        }
        return (Collection<?>) value.getObject();
    }

    /**
     * If metadata field has a value and regular field hasn't got value, build an Exp to query by metadata using
     * information set in the given qualifier map.
     *
     * @param qualifierMap Map with qualifier data
     * @return Optional with the Exp or Optional.empty()
     */
    private static Optional<Exp> getMetadataExp(Map<QualifierKey, Object> qualifierMap) {
        CriteriaDefinition.AerospikeMetadata metadataField = getMetadataField(qualifierMap);
        String binName = getBinName(qualifierMap);

        if (metadataField != null && (binName == null || binName.isEmpty())) {
            FilterOperation operation = getOperation(qualifierMap);
            switch (operation) {
                case EQ, NOTEQ, LT, LTEQ, GT, GTEQ -> {
                    BinaryOperator<Exp> operationFunction = mapOperation(operation);
                    return Optional.of(
                        operationFunction.apply(
                            mapMetadataExp(metadataField, getServerVersionSupport(qualifierMap)),
                            Exp.val(getValue(qualifierMap).toLong()) // previously validated
                        )
                    );
                }
                case BETWEEN -> {
                    Exp metadata = mapMetadataExp(metadataField, getServerVersionSupport(qualifierMap));
                    Exp value = Exp.val(getValue(qualifierMap).toLong()); // previously validated
                    Exp secondValue = Exp.val(getSecondValue(qualifierMap).toLong());
                    return Optional.of(Exp.and(Exp.ge(metadata, value), Exp.lt(metadata, secondValue)));
                }
                case IN -> {
                    return Optional.of(processMetadataFieldIn(qualifierMap));
                }
                case NOT_IN -> {
                    return Optional.of(processMetadataFieldNotIn(qualifierMap));
                }
                default -> throw new IllegalStateException("Unsupported FilterOperation " + operation);
            }
        }
        return Optional.empty();
    }

    private static Exp mapMetadataExp(CriteriaDefinition.AerospikeMetadata metadataField,
                                      ServerVersionSupport versionSupport) {
        return switch (metadataField) {
            case SINCE_UPDATE_TIME -> Exp.sinceUpdate();
            case LAST_UPDATE_TIME -> Exp.lastUpdate();
            case VOID_TIME -> Exp.voidTime();
            case TTL -> Exp.ttl();
            case RECORD_SIZE_ON_DISK -> versionSupport.isServerVersionGtOrEq7() ? Exp.recordSize() : Exp.deviceSize();
            case RECORD_SIZE_IN_MEMORY -> versionSupport.isServerVersionGtOrEq7() ? Exp.recordSize() : Exp.memorySize();
            default -> throw new IllegalStateException("Cannot map metadata Expression to " + metadataField);
        };
    }

    private static BinaryOperator<Exp> mapOperation(FilterOperation operation) {
        return switch (operation) {
            case EQ -> Exp::eq;
            case NOTEQ -> Exp::ne;
            case GT -> Exp::gt;
            case GTEQ -> Exp::ge;
            case LT -> Exp::lt;
            case LTEQ -> Exp::le;
            default -> throw new IllegalStateException("Cannot map FilterOperation from " + operation);
        };
    }

    private static Exp mapValBetweenByKey(Map<QualifierKey, Object> qualifierMap, CTX[] ctxArr,
                                          Exp.Type type,
                                          Exp lowerLimit,
                                          Exp upperLimit) {
        Exp mapExp;
        if (ctxArr != null && ctxArr.length > 0) {
            mapExp = MapExp.getByKey(MapReturnType.VALUE, type, Exp.val(getKey(qualifierMap).toString()),
                Exp.mapBin(getBinName(qualifierMap)), ctxArr);
        } else {
            mapExp = MapExp.getByKey(MapReturnType.VALUE, type, Exp.val(getKey(qualifierMap).toString()),
                Exp.mapBin(getBinName(qualifierMap)));
        }

        return Exp.and(
            Exp.ge(mapExp, lowerLimit),
            Exp.lt(mapExp, upperLimit)
        );
    }

    private static Exp mapKeysNotContain(Map<QualifierKey, Object> qualifierMap) {
        String errMsg = "MAP_KEYS_NOT_CONTAIN FilterExpression unsupported type: got " +
            getKey(qualifierMap).getClass().getSimpleName();
        Exp mapKeysNotContain = mapKeysCountComparedToZero(qualifierMap, Exp::eq, getValue(qualifierMap), errMsg);
        Exp binDoesNotExist = Exp.not(Exp.binExists(getBinName(qualifierMap)));
        return Exp.or(binDoesNotExist, mapKeysNotContain);
    }

    private static Exp mapKeysContain(Map<QualifierKey, Object> qualifierMap) {
        String errMsg = "MAP_KEYS_CONTAIN FilterExpression unsupported type: got " +
            getValue(qualifierMap).getClass().getSimpleName();
        String[] dotPathArray = getDotPathArray(getDotPath(qualifierMap));
        if (hasMapKeyPlaceholder(qualifierMap) && dotPathArray != null && dotPathArray.length > 1) {
            List<String> ctxList = convertToStringListExclStart(dotPathArray);
            qualifierMap.put(CTX_ARRAY, resolveCtxList(ctxList));
        }
        return mapKeysCountComparedToZero(qualifierMap, Exp::gt, getValue(qualifierMap), errMsg);
    }

    private static Exp mapValuesNotContain(Map<QualifierKey, Object> qualifierMap) {
        String errMsg = "MAP_VALUES_NOT_CONTAIN FilterExpression unsupported type: got " +
            getValue(qualifierMap).getClass().getSimpleName();
        Exp binDoesNotExist = Exp.not(Exp.binExists(getBinName(qualifierMap)));
        Exp mapValuesNotContain = mapValuesCountComparedToZero(qualifierMap, Exp::eq, errMsg);
        return Exp.or(binDoesNotExist, mapValuesNotContain);
    }

    private static Exp mapValuesContain(Map<QualifierKey, Object> qualifierMap) {
        String errMsg = "MAP_VALUES_CONTAIN FilterExpression unsupported type: got " +
            getValue(qualifierMap).getClass().getSimpleName();
        return mapValuesCountComparedToZero(qualifierMap, Exp::gt, errMsg);
    }

    // operator is Exp::gt to query for mapKeysContain or Exp::eq to query for mapKeysNotContain
    private static Exp mapKeysCountComparedToZero(Map<QualifierKey, Object> qualifierMap, BinaryOperator<Exp> operator,
                                                  Object mapKey, String errMsg) {
        Exp key = getExpValOrFail(Value.get(mapKey), errMsg);
        Exp map = Exp.mapBin(getBinName(qualifierMap));
        CTX[] ctxArr = getCtxArr(qualifierMap);

        return operator.apply(
            MapExp.getByKey(MapReturnType.COUNT, Exp.Type.INT, key, map, ctxArr),
            Exp.val(0));
    }

    // operator is Exp::gt to query for mapValuesContain or Exp::eq to query for mapValuesNotContain
    private static Exp mapValuesCountComparedToZero(Map<QualifierKey, Object> qualifierMap,
                                                    BinaryOperator<Exp> operator,
                                                    String errMsg) {
        Exp value = getExpValOrFail(getValue(qualifierMap), errMsg);
        Exp map = Exp.mapBin(getBinName(qualifierMap));
        Value mapBinKey = getKey(qualifierMap);

        if (!mapBinKey.equals(Value.NullValue.INSTANCE)) {
            // If map key != null it means a nested query (one level)
            String err = "MAP_VAL_NOT_CONTAINING_BY_KEY FilterExpression unsupported type: got " +
                mapBinKey.getClass().getSimpleName();
            Exp nestedMapKey = getExpValOrFail(mapBinKey, err);

            // if it is a nested query we need to locate a Map within its parent bin
            map = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.MAP, nestedMapKey,
                Exp.mapBin(getBinName(qualifierMap)));
        }

        return operator.apply(
            MapExp.getByValue(MapReturnType.COUNT, value, map),
            Exp.val(0));
    }

    private static Exp getFilterExpMapValOrFail(Map<QualifierKey, Object> qualifierMap, BinaryOperator<Exp> operator,
                                                String opName) {
        CTX[] ctxArr = getCtxArr(qualifierMap);

        return switch (getValue(qualifierMap).getType()) {
            case INTEGER -> operator.apply(getMapExp(qualifierMap, ctxArr, Exp.Type.INT),
                Exp.val(getValue(qualifierMap).toLong()));
            case STRING -> operator.apply(getMapExp(qualifierMap, ctxArr, Exp.Type.STRING),
                Exp.val(getValue(qualifierMap).toString()));
            case LIST -> operator.apply(getMapExp(qualifierMap, ctxArr, Exp.Type.LIST),
                Exp.val((List<?>) getValue(qualifierMap).getObject()));
            case MAP -> operator.apply(getMapExp(qualifierMap, ctxArr, Exp.Type.MAP),
                Exp.val((Map<?, ?>) getValue(qualifierMap).getObject()));
            default -> throw new UnsupportedOperationException(
                opName + " FilterExpression unsupported type: " + getValue(qualifierMap).getClass().getSimpleName());
        };
    }

    private static Exp getMapExp(Map<QualifierKey, Object> qualifierMap, CTX[] ctxArr, Exp.Type expType) {
        Exp mapKeyExp = getMapKeyExp(getKey(qualifierMap).getObject(), keepOriginalKeyTypes(qualifierMap));
        if (ctxArr != null) {
            return MapExp.getByKey(MapReturnType.VALUE, expType, mapKeyExp,
                Exp.mapBin(getBinName(qualifierMap)), ctxArr);
        } else {
            return MapExp.getByKey(MapReturnType.VALUE, expType, mapKeyExp,
                Exp.mapBin(getBinName(qualifierMap)));
        }
    }

    private static Exp getMapKeyExp(Object mapKey, boolean keepOriginalKeyTypes) {
        // Choosing whether to preserve map key type based on the configuration
        if (keepOriginalKeyTypes) {
            Exp res;
            if (mapKey instanceof Byte || mapKey instanceof Short || mapKey instanceof Integer
                || mapKey instanceof Long) {
                res = Exp.val(((Number) mapKey).longValue());
            } else if (mapKey instanceof Float || mapKey instanceof Double) {
                res = Exp.val(((Number) mapKey).doubleValue());
            } else if (mapKey instanceof byte[]) {
                res = Exp.val((byte[]) mapKey);
            } else if (Value.get(mapKey) instanceof Value.NullValue) {
                res = Exp.nil();
            } else {
                res = Exp.val(Value.get(mapKey).toString());
            }
            return res;
        }
        return Exp.val(Value.get(mapKey).toString());
    }

    private static boolean keepOriginalKeyTypes(Map<QualifierKey, Object> qualifierMap) {
        Object dataSettings = qualifierMap.get(DATA_SETTINGS);
        if (dataSettings == null) throw new IllegalStateException("Expecting AerospikeDataSettings in qualifier map " +
            "with the key " + DATA_SETTINGS);
        return ((AerospikeDataSettings) dataSettings).isKeepOriginalKeyTypes();
    }

    private static Exp getFilterExpMapValEqOrFail(Map<QualifierKey, Object> qualifierMap,
                                                  BinaryOperator<Exp> operator) {
        return getMapValEqOrFail(qualifierMap, operator, "MAP_VAL_EQ_BY_KEY");
    }

    private static Exp getFilterExpMapValNotEqOrFail(Map<QualifierKey, Object> qualifierMap,
                                                     BinaryOperator<Exp> operator) {
        return getMapValEqOrFail(qualifierMap, operator, "MAP_VAL_NOTEQ_BY_KEY");
    }

    private static Exp getMapValEqOrFail(Map<QualifierKey, Object> qualifierMap, BinaryOperator<Exp> operator,
                                         String opName) {
        CTX[] ctxArr = getCtxArr(qualifierMap);

        // boolean values are read as BoolIntValue (INTEGER ParticleType) if Value.UseBoolBin == false
        // so converting to BooleanValue to process correctly
        if (getValue(qualifierMap) instanceof Value.BoolIntValue) {
            qualifierMap.put(VALUE, new Value.BooleanValue((Boolean) getValue(qualifierMap).getObject()));
        }

        Value value = getValue(qualifierMap);
        return switch (value.getType()) {
            case INTEGER -> getMapValEqExp(qualifierMap, Exp.Type.INT, value.toLong(), ctxArr, operator);
            case STRING -> {
                if (ignoreCase(qualifierMap)) {
                    throw new UnsupportedOperationException(
                        opName + " FilterExpression: case insensitive comparison is not supported");
                }
                yield getMapValEqExp(qualifierMap, Exp.Type.STRING, value.toString(), ctxArr, operator);
            }
            case BOOL -> getMapValEqExp(qualifierMap, Exp.Type.BOOL, value.getObject(), ctxArr, operator);
            case LIST -> getMapValEqExp(qualifierMap, Exp.Type.LIST, value.getObject(), ctxArr, operator);
            case MAP -> getMapValEqExp(qualifierMap, Exp.Type.MAP, value.getObject(), ctxArr, operator);
            default -> throw new UnsupportedOperationException(
                opName + " FilterExpression unsupported type: " + value.getClass().getSimpleName());
        };
    }

    private static Exp getMapValEqExp(Map<QualifierKey, Object> qualifierMap, Exp.Type expType, Object value,
                                      CTX[] ctxArr, BinaryOperator<Exp> operator) {
        Exp mapExp = getMapValEq(qualifierMap, expType, ctxArr);
        return operator.apply(mapExp, toExp(value));
    }

    private static Exp getMapValEq(Map<QualifierKey, Object> qualifierMap, Exp.Type expType, CTX[] ctxArr) {
        Exp bin = getBin(getBinName(qualifierMap), getBinType(qualifierMap), "MAP_VAL_EQ");
        if (ctxArr != null && ctxArr.length > 0) {
            return MapExp.getByKey(MapReturnType.VALUE, expType,
                getExpValOrFail(getKey(qualifierMap), "MAP_VAL_EQ: unsupported type"),
                bin, ctxArr);
        } else {
            return MapExp.getByKey(MapReturnType.VALUE, expType,
                getExpValOrFail(getKey(qualifierMap), "MAP_VAL_EQ: unsupported type"),
                bin);
        }
    }

    private static Exp getBin(String binName, Exp.Type binType, String filterOperation) {
        if (binType == null) {
            return Exp.mapBin(binName);
        }

        return switch (binType) {
            case INT -> Exp.intBin(binName);
            case STRING -> Exp.stringBin(binName);
            case BOOL -> Exp.boolBin(binName);
            case LIST -> Exp.listBin(binName);
            default -> Exp.mapBin(binName); // currently mapBin is the default
        };
    }

    private static Exp getFilterExp(Exp exp, String field,
                                    BinaryOperator<Exp> operator, Function<String, Exp> binExp) {
        return operator.apply(binExp.apply(field), exp);
    }

    private static Exp toExp(Object value) {
        Exp res;
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            res = Exp.val(((Number) value).longValue());
        } else if (value instanceof Float || value instanceof Double) {
            res = Exp.val(((Number) value).doubleValue());
        } else if (value instanceof String) {
            res = Exp.val((String) value);
        } else if (value instanceof Boolean) {
            res = Exp.val((Boolean) value);
        } else if (value instanceof Map<?, ?>) {
            res = Exp.val((Map<?, ?>) value);
        } else if (value instanceof List<?>) {
            res = Exp.val((List<?>) value);
        } else if (value instanceof byte[]) {
            res = Exp.val((byte[]) value);
        } else if (value instanceof Calendar) {
            res = Exp.val((Calendar) value);
        } else if (value instanceof Value.NullValue) {
            res = Exp.nil();
        } else {
            throw new UnsupportedOperationException("Unsupported type for converting: " + value.getClass()
                .getCanonicalName());
        }

        return res;
    }

    protected static String getBinName(Map<QualifierKey, Object> qualifierMap) {
        return (String) qualifierMap.get(BIN_NAME);
    }

    protected static Exp.Type getBinType(Map<QualifierKey, Object> qualifierMap) {
        return (Exp.Type) qualifierMap.get(BIN_TYPE);
    }

    protected static CriteriaDefinition.AerospikeMetadata getMetadataField(Map<QualifierKey, Object> qualifierMap) {
        return (CriteriaDefinition.AerospikeMetadata) qualifierMap.get(METADATA_FIELD);
    }

    protected static FilterOperation getOperation(Map<QualifierKey, Object> qualifierMap) {
        return (FilterOperation) qualifierMap.get(FILTER_OPERATION);
    }

    protected static Boolean ignoreCase(Map<QualifierKey, Object> qualifierMap) {
        return (Boolean) qualifierMap.getOrDefault(IGNORE_CASE, false);
    }

    protected static int regexFlags(Map<QualifierKey, Object> qualifierMap) {
        return ignoreCase(qualifierMap) ? RegexFlag.ICASE : RegexFlag.NONE;
    }

    protected static Value getKey(Map<QualifierKey, Object> qualifierMap) {
        return Value.get(qualifierMap.get(KEY));
    }

    protected static Value getNestedKey(Map<QualifierKey, Object> qualifierMap) {
        return Value.get(qualifierMap.get(NESTED_KEY));
    }

    protected static Value getValue(Map<QualifierKey, Object> qualifierMap) {
        return Value.get(qualifierMap.get(VALUE));
    }

    protected static Value getSecondValue(Map<QualifierKey, Object> qualifierMap) {
        return Value.get(qualifierMap.get(SECOND_VALUE));
    }

    protected static Integer getNestedType(Map<QualifierKey, Object> qualifierMap) {
        Object fieldType = qualifierMap.get(NESTED_TYPE);
        return fieldType != null ? (int) fieldType : null;
    }

    protected static String getSecondaryIndexName(Map<QualifierKey, Object> qualifierMap) {
        return (String) qualifierMap.get(SINDEX_NAME);
    }

    protected static boolean hasId(Map<QualifierKey, Object> qualifierMap) {
        return qualifierMap.containsKey(IS_ID_EXPR) && (Boolean) qualifierMap.get(IS_ID_EXPR);
    }

    protected static boolean hasSecondaryIndexName(Map<QualifierKey, Object> qualifierMap) {
        return qualifierMap.containsKey(SINDEX_NAME);
    }

    protected static CTX[] getCtxArr(Map<QualifierKey, Object> qualifierMap) {
        return (CTX[]) qualifierMap.get(CTX_ARRAY);
    }

    protected static String getCtxArrAsString(Map<QualifierKey, Object> qualifierMap) {
        CTX[] ctxArr = (CTX[]) qualifierMap.get(CTX_ARRAY);
        return ctxArrToString(ctxArr);
    }

    protected static boolean hasServerVersionSupport(Map<QualifierKey, Object> qualifierMap) {
        return qualifierMap.containsKey(SERVER_VERSION_SUPPORT);
    }

    protected static ServerVersionSupport getServerVersionSupport(Map<QualifierKey, Object> qualifierMap) {
        return (ServerVersionSupport) qualifierMap.get(SERVER_VERSION_SUPPORT);
    }

    public abstract Exp filterExp(Map<QualifierKey, Object> qualifierMap);

    public abstract Filter sIndexFilter(Map<QualifierKey, Object> qualifierMap);

    protected Filter cdtContains(IndexCollectionType collectionType, Map<QualifierKey, Object> qualifierMap,
                                 boolean hasIndexName) {
        Value val = getValue(qualifierMap);
        int valType = val.getType();
        String[] dotPathArray = getDotPathArray(getDotPath(qualifierMap));
        if (dotPathArray != null && dotPathArray.length > 1) {
            List<String> ctxList = convertToStringListExclStart(dotPathArray);
            qualifierMap.put(CTX_ARRAY, resolveCtxList(ctxList));
        }
        return switch (valType) {
            case INTEGER -> hasIndexName
                ? Filter.containsByIndex(getSecondaryIndexName(qualifierMap), collectionType, val.toLong())
                : Filter.contains(getBinName(qualifierMap), collectionType, val.toLong(), getCtxArr(qualifierMap));
            case STRING -> hasIndexName
                ? Filter.containsByIndex(getSecondaryIndexName(qualifierMap), collectionType, val.toString())
                : Filter.contains(getBinName(qualifierMap), collectionType, val.toString(), getCtxArr(qualifierMap));
            case BLOB -> hasIndexName
                ? Filter.containsByIndex(getSecondaryIndexName(qualifierMap), collectionType, (byte[]) val.getObject())
                : Filter.contains(getBinName(qualifierMap), collectionType, (byte[]) val.getObject(),
                getCtxArr(qualifierMap));
            default -> null;
        };
    }

    @SuppressWarnings("unchecked")
    protected static List<String> getDotPath(Map<QualifierKey, Object> qualifierMap) {
        return (List<String>) qualifierMap.get(DOT_PATH);
    }

    protected Filter collectionRange(IndexCollectionType collectionType, Map<QualifierKey, Object> qualifierMap,
                                     boolean hasIndexName) {
        return hasIndexName
        ? Filter.rangeByIndex(getSecondaryIndexName(qualifierMap), collectionType, getValue(qualifierMap).toLong(),
            getSecondValue(qualifierMap).toLong())
        : Filter.range(getBinName(qualifierMap), collectionType, getValue(qualifierMap).toLong(),
            getSecondValue(qualifierMap).toLong());
    }

    @SuppressWarnings("SameParameterValue")
    protected Filter geoWithinRadius(IndexCollectionType collectionType, Map<QualifierKey, Object> qualifierMap,
                                     boolean hasIndexName) {
        return hasIndexName
        ? Filter.geoContainsByIndex(getSecondaryIndexName(qualifierMap), getValue(qualifierMap).toString())
        : Filter.geoContains(getBinName(qualifierMap), getValue(qualifierMap).toString());
    }

    private static boolean hasMapKeyPlaceholder(Map<QualifierKey, Object> qualifierMap) {
        return (Boolean) qualifierMap.getOrDefault(MAP_KEY_PLACEHOLDER, false);
    }
}
