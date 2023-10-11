package org.springframework.data.aerospike.query;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.RegexFlag;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition;
import org.springframework.data.util.Pair;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import static com.aerospike.client.command.ParticleType.BOOL;
import static com.aerospike.client.command.ParticleType.INTEGER;
import static com.aerospike.client.command.ParticleType.LIST;
import static com.aerospike.client.command.ParticleType.MAP;
import static com.aerospike.client.command.ParticleType.NULL;
import static com.aerospike.client.command.ParticleType.STRING;
import static org.springframework.data.aerospike.query.Qualifier.*;

public enum FilterOperation {

    AND {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            Qualifier[] qs = (Qualifier[]) qualifierMap.get(QUALIFIERS);
            Exp[] childrenExp = new Exp[qs.length];
            for (int i = 0; i < qs.length; i++) {
                childrenExp[i] = qs[i].toFilterExp();
            }
            return Exp.and(childrenExp);
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null;
        }
    },
    OR {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            Qualifier[] qs = (Qualifier[]) qualifierMap.get(QUALIFIERS);
            Exp[] childrenExp = new Exp[qs.length];
            for (int i = 0; i < qs.length; i++) {
                childrenExp[i] = qs[i].toFilterExp();
            }
            return Exp.or(childrenExp);
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null;
        }
    },
    IN {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            // Convert IN to a collection of OR as Aerospike has no direct support for IN query
            return getMetadataExp(qualifierMap).orElseGet(() -> {
                Value value1 = getValue1AsCollectionOrFail(qualifierMap);
                Collection<?> collection = (Collection<?>) value1.getObject();
                Exp[] arrElementsExp = collection.stream().map(item ->
                    new Qualifier(
                        new QualifierBuilder()
                            .setField(getField(qualifierMap))
                            .setFilterOperation(FilterOperation.EQ)
                            .setValue1(Value.get(item))
                    ).toFilterExp()
                ).toArray(Exp[]::new);

                return Exp.or(arrElementsExp);
            });
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null;
        }
    },
    NOT_IN {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            // Convert NOT_IN to a collection of AND as Aerospike has no direct support for IN query

            return getMetadataExp(qualifierMap).orElseGet(() -> {
                Value value1 = getValue1AsCollectionOrFail(qualifierMap);

                Collection<?> collection = (Collection<?>) value1.getObject();
                Exp[] arrElementsExp = collection.stream().map(item ->
                    new Qualifier(
                        new QualifierBuilder()
                            .setField(getField(qualifierMap))
                            .setFilterOperation(FilterOperation.NOTEQ)
                            .setValue1(Value.get(item))
                    ).toFilterExp()
                ).toArray(Exp[]::new);

                return Exp.and(arrElementsExp);
            });
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null;
        }
    },
    EQ {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            return getMetadataExp(qualifierMap).orElseGet(() -> {
                Value value = getValue1(qualifierMap);
                return switch (value.getType()) {
                    case INTEGER -> Exp.eq(Exp.intBin(getField(qualifierMap)), Exp.val(value.toLong()));
                    case STRING -> {
                        if (ignoreCase(qualifierMap)) {
                            String equalsRegexp =
                                QualifierRegexpBuilder.getStringEquals(getValue1(qualifierMap).toString());
                            yield Exp.regexCompare(equalsRegexp, RegexFlag.ICASE,
                                Exp.stringBin(getField(qualifierMap)));
                        } else {
                            yield Exp.eq(Exp.stringBin(getField(qualifierMap)), Exp.val(value.toString()));
                        }
                    }
                    case BOOL -> Exp.eq(Exp.boolBin(getField(qualifierMap)), Exp.val((Boolean) value.getObject()));
                    case MAP -> getFilterExp(Exp.val((Map<?, ?>) value.getObject()), getField(qualifierMap), Exp::eq,
                        Exp::mapBin);
                    case LIST ->
                        getFilterExp(Exp.val((List<?>) value.getObject()), getField(qualifierMap), Exp::eq,
                            Exp::listBin);
                    default -> throw new IllegalArgumentException("EQ FilterExpression unsupported particle type: " +
                        value.getClass().getSimpleName());
                };
            });
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            if (getValue1(qualifierMap).getType() == INTEGER) {
                return Filter.equal(getField(qualifierMap), getValue1(qualifierMap).toLong());
            } else {
                // There is no case-insensitive string comparison filter.
                if (ignoreCase(qualifierMap)) {
                    return null;
                }
                return Filter.equal(getField(qualifierMap), getValue1(qualifierMap).toString());
            }
        }
    },
    NOTEQ {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            return getMetadataExp(qualifierMap).orElseGet(() -> {
                Value value = getValue1(qualifierMap);
                return switch (value.getType()) {
                    // FMWK-175: Exp.ne() does not return null bins, so Exp.not(Exp.binExists()) is added
                    case INTEGER -> {
                        Exp ne = Exp.ne(Exp.intBin(getField(qualifierMap)), Exp.val(value.toLong()));
                        yield Exp.or(Exp.not(Exp.binExists(getField(qualifierMap))), ne);
                    }
                    case STRING -> {
                        if (ignoreCase(qualifierMap)) {
                            String equalsRegexp =
                                QualifierRegexpBuilder.getStringEquals(getValue1(qualifierMap).toString());
                            Exp regexCompare = Exp.not(Exp.regexCompare(equalsRegexp, RegexFlag.ICASE,
                                Exp.stringBin(getField(qualifierMap))));
                            yield Exp.or(Exp.not(Exp.binExists(getField(qualifierMap))), regexCompare);
                        } else {
                            Exp ne = Exp.ne(Exp.stringBin(getField(qualifierMap)), Exp.val(value.toString()));
                            yield Exp.or(Exp.not(Exp.binExists(getField(qualifierMap))), ne);
                        }
                    }
                    case BOOL -> {
                        Exp ne = Exp.ne(Exp.boolBin(getField(qualifierMap)), Exp.val((Boolean) value.getObject()));
                        yield Exp.or(Exp.not(Exp.binExists(getField(qualifierMap))), ne);
                    }
                    case MAP -> getFilterExp(Exp.val((Map<?, ?>) value.getObject()), getField(qualifierMap), Exp::ne,
                        Exp::mapBin);
                    case LIST ->
                        getFilterExp(Exp.val((List<?>) value.getObject()), getField(qualifierMap), Exp::ne,
                            Exp::listBin);
                    default -> throw new IllegalArgumentException("NOTEQ FilterExpression unsupported particle type: " +
                        value.getClass().getSimpleName());
                };
            });
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null; // not supported in the secondary index filter
        }
    },
    GT {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            return getMetadataExp(qualifierMap).orElseGet(() -> {
                Value value = getValue1(qualifierMap);
                return switch (value.getType()) {
                    case INTEGER ->
                        Exp.gt(Exp.intBin(getField(qualifierMap)), Exp.val(getValue1(qualifierMap).toLong()));
                    case STRING ->
                        Exp.gt(Exp.stringBin(getField(qualifierMap)), Exp.val(getValue1(qualifierMap).toString()));
                    case MAP -> getFilterExp(Exp.val((Map<?, ?>) value.getObject()), getField(qualifierMap), Exp::gt,
                        Exp::mapBin);
                    case LIST ->
                        getFilterExp(Exp.val((List<?>) value.getObject()), getField(qualifierMap), Exp::gt,
                            Exp::listBin);
                    default -> throw new IllegalArgumentException("GT FilterExpression unsupported particle type: " +
                        value.getClass().getSimpleName());
                };
            });
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            // Long.MAX_VALUE shall not be given as + 1 will cause overflow
            if (getValue1(qualifierMap).getType() != INTEGER || getValue1(qualifierMap).toLong() == Long.MAX_VALUE) {
                return null;
            }

            return Filter.range(getField(qualifierMap), getValue1(qualifierMap).toLong() + 1, Long.MAX_VALUE);
        }
    },
    GTEQ {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            return getMetadataExp(qualifierMap).orElseGet(() -> {
                Value value = getValue1(qualifierMap);
                return switch (value.getType()) {
                    case INTEGER ->
                        Exp.ge(Exp.intBin(getField(qualifierMap)), Exp.val(getValue1(qualifierMap).toLong()));
                    case STRING ->
                        Exp.ge(Exp.stringBin(getField(qualifierMap)), Exp.val(getValue1(qualifierMap).toString()));
                    case MAP -> getFilterExp(Exp.val((Map<?, ?>) value.getObject()), getField(qualifierMap), Exp::ge,
                        Exp::mapBin);
                    case LIST ->
                        getFilterExp(Exp.val((List<?>) value.getObject()), getField(qualifierMap), Exp::ge,
                            Exp::listBin);
                    default -> throw new IllegalArgumentException("GTEQ FilterExpression unsupported particle type: " +
                        value.getClass().getSimpleName());
                };
            });
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            if (getValue1(qualifierMap).getType() != INTEGER) {
                return null;
            }
            return Filter.range(getField(qualifierMap), getValue1(qualifierMap).toLong(), Long.MAX_VALUE);
        }
    },
    LT {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            return getMetadataExp(qualifierMap).orElseGet(() -> {
                Value value = getValue1(qualifierMap);
                return switch (value.getType()) {
                    case INTEGER ->
                        Exp.lt(Exp.intBin(getField(qualifierMap)), Exp.val(getValue1(qualifierMap).toLong()));
                    case STRING ->
                        Exp.lt(Exp.stringBin(getField(qualifierMap)), Exp.val(getValue1(qualifierMap).toString()));
                    case MAP -> getFilterExp(Exp.val((Map<?, ?>) value.getObject()), getField(qualifierMap), Exp::lt,
                        Exp::mapBin);
                    case LIST ->
                        getFilterExp(Exp.val((List<?>) value.getObject()), getField(qualifierMap), Exp::lt,
                            Exp::listBin);
                    default -> throw new IllegalArgumentException("LT FilterExpression unsupported particle type: " +
                        value.getClass().getSimpleName());
                };
            });
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            // Long.MIN_VALUE shall not be given as - 1 will cause overflow
            if (getValue1(qualifierMap).getType() != INTEGER || getValue1(qualifierMap).toLong() == Long.MIN_VALUE) {
                return null;
            }
            return Filter.range(getField(qualifierMap), Long.MIN_VALUE, getValue1(qualifierMap).toLong() - 1);
        }
    },
    LTEQ {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            return getMetadataExp(qualifierMap).orElseGet(() -> {
                Value value = getValue1(qualifierMap);
                return switch (value.getType()) {
                    case INTEGER ->
                        Exp.le(Exp.intBin(getField(qualifierMap)), Exp.val(getValue1(qualifierMap).toLong()));
                    case STRING ->
                        Exp.le(Exp.stringBin(getField(qualifierMap)), Exp.val(getValue1(qualifierMap).toString()));
                    case MAP -> getFilterExp(Exp.val((Map<?, ?>) value.getObject()), getField(qualifierMap), Exp::le,
                        Exp::mapBin);
                    case LIST ->
                        getFilterExp(Exp.val((List<?>) value.getObject()), getField(qualifierMap), Exp::le,
                            Exp::listBin);
                    default -> throw new IllegalArgumentException("LTEQ FilterExpression unsupported particle type: " +
                        value.getClass().getSimpleName());
                };
            });
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            if (getValue1(qualifierMap).getType() != INTEGER) {
                return null;
            }
            return Filter.range(getField(qualifierMap), Long.MIN_VALUE, getValue1(qualifierMap).toLong());
        }
    },
    BETWEEN {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            validateEquality(getValue1(qualifierMap).getType(), getValue2(qualifierMap).getType(), qualifierMap,
                "BETWEEN");

            return getMetadataExp(qualifierMap).orElseGet(() -> switch (getValue1(qualifierMap).getType()) {
                case INTEGER -> Exp.and(
                    Exp.ge(Exp.intBin(getField(qualifierMap)), Exp.val(getValue1(qualifierMap).toLong())),
                    Exp.lt(Exp.intBin(getField(qualifierMap)), Exp.val(getValue2(qualifierMap).toLong()))
                );
                case STRING -> Exp.and(
                    Exp.ge(Exp.stringBin(getField(qualifierMap)), Exp.val(getValue1(qualifierMap).toString())),
                    Exp.lt(Exp.stringBin(getField(qualifierMap)), Exp.val(getValue2(qualifierMap).toString()))
                );
                case MAP -> Exp.and(
                    getFilterExp(Exp.val((Map<?, ?>) getValue1(qualifierMap).getObject()),
                        getField(qualifierMap), Exp::ge, Exp::mapBin),
                    getFilterExp(Exp.val((Map<?, ?>) getValue2(qualifierMap).getObject()),
                        getField(qualifierMap), Exp::lt, Exp::mapBin)
                );
                case LIST -> Exp.and(
                    getFilterExp(Exp.val((List<?>) getValue1(qualifierMap).getObject()), getField(qualifierMap),
                        Exp::ge, Exp::listBin),
                    getFilterExp(Exp.val((List<?>) getValue2(qualifierMap).getObject()), getField(qualifierMap),
                        Exp::lt, Exp::listBin)
                );
                default ->
                    throw new IllegalArgumentException("BETWEEN: unexpected value of type " + getValue1(qualifierMap).getClass()
                        .getSimpleName());
            });
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            if (getValue1(qualifierMap).getType() != INTEGER || getValue2(qualifierMap).getType() != INTEGER) {
                return null;
            }
            return Filter.range(getField(qualifierMap), getValue1(qualifierMap).toLong(),
                getValue2(qualifierMap).toLong());
        }
    },
    STARTS_WITH {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            String startWithRegexp = QualifierRegexpBuilder.getStartsWith(getValue1(qualifierMap).toString());
            return Exp.regexCompare(startWithRegexp, regexFlags(qualifierMap), Exp.stringBin(getField(qualifierMap)));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null; // String secondary index does not support "starts with" queries
        }
    },
    ENDS_WITH {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            String endWithRegexp = QualifierRegexpBuilder.getEndsWith(getValue1(qualifierMap).toString());
            return Exp.regexCompare(endWithRegexp, regexFlags(qualifierMap), Exp.stringBin(getField(qualifierMap)));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null; // String secondary index does not support "ends with" queries
        }
    },
    CONTAINING {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            String containingRegexp = QualifierRegexpBuilder.getContaining(getValue1(qualifierMap).toString());
            return Exp.regexCompare(containingRegexp, regexFlags(qualifierMap), Exp.stringBin(getField(qualifierMap)));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null; // String secondary index does not support "contains" queries
        }
    },
    NOT_CONTAINING {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            String notContainingRegexp = QualifierRegexpBuilder.getNotContaining(getValue1(qualifierMap).toString());
            return Exp.or(Exp.not(Exp.binExists(getField(qualifierMap))),
                Exp.not(Exp.regexCompare(notContainingRegexp, regexFlags(qualifierMap),
                    Exp.stringBin(getField(qualifierMap)))));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null; // String secondary index does not support "contains" queries
        }
    },
    LIKE {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            int flags = RegexFlag.EXTENDED;
            if (ignoreCase(qualifierMap)) {
                flags = RegexFlag.EXTENDED | RegexFlag.ICASE;
            }
            return Exp.regexCompare(getValue1(qualifierMap).toString(), flags, Exp.stringBin(getField(qualifierMap)));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null; // not supported
        }
    },
    MAP_VAL_EQ_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            return getFilterExpMapValEqOrFail(qualifierMap, Exp::eq);
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            String[] dotPathArr = getDotPathArray(getDotPath(qualifierMap),
                "MAP_VAL_EQ_BY_KEY secondary index filter: dotPath has not been set");
            final boolean useCtx = dotPathArr.length > 2;

            return switch (getValue1(qualifierMap).getType()) {
                case STRING -> {
                    if (ignoreCase(qualifierMap)) { // there is no case-insensitive string comparison filter
                        yield null; // MAP_VALUE_EQ_BY_KEY sIndexFilter: case-insensitive comparison is not supported
                    }
                    if (useCtx) {
                        yield null; // currently not supported
                    } else {
                        yield Filter.contains(getField(qualifierMap), IndexCollectionType.MAPVALUES,
                            getValue1(qualifierMap).toString());
                    }
                }
                case INTEGER -> {
                    if (useCtx) {
                        yield null; // currently not supported
                    } else {
                        yield Filter.range(getField(qualifierMap), IndexCollectionType.MAPVALUES,
                            getValue1(qualifierMap).toLong(),
                            getValue1(qualifierMap).toLong());
                    }
                }
                default -> null;
            };
        }
    },
    MAP_VAL_NOTEQ_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            return getFilterExpMapValNotEqOrFail(qualifierMap, Exp::ne);
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null; // not supported
        }
    },
    MAP_VAL_GT_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            return getFilterExpMapValOrFail(qualifierMap, Exp::gt, "MAP_VAL_GT_BY_KEY");
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            // Long.MAX_VALUE shall not be given as + 1 will cause overflow
            if (getValue1(qualifierMap).getType() != INTEGER || getValue1(qualifierMap).toLong() == Long.MAX_VALUE) {
                return null;
            }

            String[] dotPathArr = getDotPathArray(getDotPath(qualifierMap),
                "MAP_VAL_GT_BY_KEY secondary index filter: dotPath has not been set");
            if (dotPathArr.length > 2) {
                return null; // currently not supported
            } else {
                return Filter.range(getField(qualifierMap), IndexCollectionType.MAPVALUES,
                    getValue1(qualifierMap).toLong() + 1,
                    Long.MAX_VALUE);
            }
        }
    },
    MAP_VAL_GTEQ_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            return getFilterExpMapValOrFail(qualifierMap, Exp::ge, "MAP_VAL_GTEQ_BY_KEY");
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            if (getValue1(qualifierMap).getType() != INTEGER) {
                return null;
            }

            String[] dotPathArr = getDotPathArray(getDotPath(qualifierMap),
                "MAP_VAL_GTEQ_BY_KEY secondary index filter: dotPath has not been set");
            if (dotPathArr.length > 2) {
                return null; // currently not supported
            } else {
                return Filter.range(getField(qualifierMap), IndexCollectionType.MAPVALUES,
                    getValue1(qualifierMap).toLong(),
                    Long.MAX_VALUE);
            }
        }
    },
    MAP_VAL_LT_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            return getFilterExpMapValOrFail(qualifierMap, Exp::lt, "MAP_VAL_LT_BY_KEY");
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            // Long.MIN_VALUE shall not be given as - 1 will cause overflow
            if (getValue1(qualifierMap).getType() != INTEGER || getValue1(qualifierMap).toLong() == Long.MIN_VALUE) {
                return null;
            }

            String[] dotPathArr = getDotPathArray(getDotPath(qualifierMap),
                "MAP_VAL_LT_BY_KEY secondary index filter: dotPath has not been set");
            if (dotPathArr.length > 2) {
                return null; // currently not supported
            } else {
                return Filter.range(getField(qualifierMap), IndexCollectionType.MAPVALUES, Long.MIN_VALUE,
                    getValue1(qualifierMap).toLong() - 1);
            }
        }
    },
    MAP_VAL_LTEQ_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            return getFilterExpMapValOrFail(qualifierMap, Exp::le, "MAP_VAL_LTEQ_BY_KEY");
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            if (getValue1(qualifierMap).getType() != INTEGER) {
                return null;
            }

            String[] dotPathArr = getDotPathArray(getDotPath(qualifierMap),
                "MAP_VAL_LTEQ_BY_KEY secondary index filter: dotPath has not been set");
            if (dotPathArr.length > 2) {
                return null; // currently not supported
            } else {
                return Filter.range(getField(qualifierMap), IndexCollectionType.MAPVALUES, Long.MIN_VALUE,
                    getValue1(qualifierMap).toLong());
            }
        }
    },
    MAP_VAL_BETWEEN_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            String[] dotPathArr = getDotPathArray(getDotPath(qualifierMap),
                "MAP_VAL_BETWEEN_BY_KEY filter expression: dotPath has not been set");

            // VALUE2 contains key (field name), VALUE3 contains upper limit
            validateEquality(getValue1(qualifierMap).getType(), getValue3(qualifierMap).getType(), qualifierMap,
                "MAP_VAL_BETWEEN_BY_KEY");

            Exp value1, value2;
            Exp.Type type;
            switch (getValue1(qualifierMap).getType()) {
                case INTEGER -> {
                    value1 = Exp.val(getValue1(qualifierMap).toLong());
                    value2 = Exp.val(getValue3(qualifierMap).toLong());
                    type = Exp.Type.INT;
                }
                case STRING -> {
                    value1 = Exp.val(getValue1(qualifierMap).toString());
                    value2 = Exp.val(getValue3(qualifierMap).toString());
                    type = Exp.Type.STRING;
                }
                case LIST -> {
                    value1 = Exp.val((List<?>) getValue1(qualifierMap).getObject());
                    value2 = Exp.val((List<?>) getValue3(qualifierMap).getObject());
                    type = Exp.Type.LIST;
                }
                case MAP -> {
                    value1 = Exp.val((Map<?, ?>) getValue1(qualifierMap).getObject());
                    value2 = Exp.val((Map<?, ?>) getValue3(qualifierMap).getObject());
                    type = Exp.Type.MAP;
                }
                default -> throw new IllegalArgumentException(
                    "MAP_VAL_BETWEEN_BY_KEY FilterExpression unsupported type: got " +
                        getValue1(qualifierMap).getClass().getSimpleName());
            }

            return mapValBetweenByKey(qualifierMap, dotPathArr, type, value1, value2);
        }

        private static Exp mapValBetweenByKey(Map<String, Object> qualifierMap, String[] dotPathArr, Exp.Type type,
                                              Exp lowerLimit,
                                              Exp upperLimit) {
            Exp mapExp;
            if (dotPathArr.length > 2) {
                mapExp = MapExp.getByKey(MapReturnType.VALUE, type, Exp.val(getValue2(qualifierMap).toString()),
                    Exp.mapBin(getField(qualifierMap)), dotPathToCtxMapKeys(dotPathArr));
            } else {
                mapExp = MapExp.getByKey(MapReturnType.VALUE, type, Exp.val(getValue2(qualifierMap).toString()),
                    Exp.mapBin(getField(qualifierMap)));
            }

            return Exp.and(
                Exp.ge(mapExp, lowerLimit),
                Exp.lt(mapExp, upperLimit)
            );
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            if (getValue1(qualifierMap).getType() != INTEGER || getValue3(qualifierMap).getType() != INTEGER) {
                return null;
            }

            String[] dotPathArr = getDotPathArray(getDotPath(qualifierMap),
                "MAP_VAL_BETWEEN_BY_KEY secondary index filter: dotPath has not been set");
            if (dotPathArr.length > 2) {
                return null; // currently not supported
            } else {
                return Filter.range(getField(qualifierMap), IndexCollectionType.MAPVALUES,
                    getValue1(qualifierMap).toLong(),
                    getValue3(qualifierMap).toLong());
            }
        }
    },
    MAP_VAL_STARTS_WITH_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            String startWithRegexp = QualifierRegexpBuilder.getStartsWith(getValue1(qualifierMap).toString());

            return Exp.regexCompare(startWithRegexp, regexFlags(qualifierMap),
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getValue2(qualifierMap).toString()),
                    Exp.mapBin(getField(qualifierMap)))
            );
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null; // String secondary index does not support "starts with" queries
        }
    },
    MAP_VAL_LIKE_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            int flags = RegexFlag.EXTENDED;
            if (ignoreCase(qualifierMap)) {
                flags = RegexFlag.EXTENDED | RegexFlag.ICASE;
            }
            return Exp.regexCompare(getValue1(qualifierMap).toString(), flags,
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getValue2(qualifierMap).toString()),
                    Exp.mapBin(getField(qualifierMap)))
            );
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null; // not supported
        }
    },
    MAP_VAL_ENDS_WITH_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            String endWithRegexp = QualifierRegexpBuilder.getEndsWith(getValue1(qualifierMap).toString());

            return Exp.regexCompare(endWithRegexp, regexFlags(qualifierMap),
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getValue2(qualifierMap).toString()),
                    Exp.mapBin(getField(qualifierMap)))
            );
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null; // String secondary index does not support "ends with" queries
        }
    },
    MAP_VAL_CONTAINING_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            String containingRegexp = QualifierRegexpBuilder.getContaining(getValue1(qualifierMap).toString());
            Exp bin = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getValue2(qualifierMap).toString()),
                Exp.mapBin(getField(qualifierMap)));
            return Exp.regexCompare(containingRegexp, regexFlags(qualifierMap), bin);
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null; // String secondary index does not support "contains" queries
        }
    },
    MAP_VAL_NOT_CONTAINING_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            String containingRegexp = QualifierRegexpBuilder.getContaining(getValue1(qualifierMap).toString());
            Exp mapIsNull = Exp.not(Exp.binExists(getField(qualifierMap)));
            Exp mapKeysNotContaining = Exp.eq(
                MapExp.getByKey(MapReturnType.COUNT, Exp.Type.INT, Exp.val(getValue2(qualifierMap).toString()),
                    Exp.mapBin(getField(qualifierMap))),
                Exp.val(0));
            Exp binValue = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                Exp.val(getValue2(qualifierMap).toString()),
                Exp.mapBin(getField(qualifierMap)));
            return Exp.or(mapIsNull, mapKeysNotContaining,
                Exp.not(Exp.regexCompare(containingRegexp, regexFlags(qualifierMap), binValue)));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null; // String secondary index does not support "contains" queries
        }
    },
    MAP_VAL_EXISTS_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            return mapKeysContain(qualifierMap);
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null;
        }
    },
    MAP_VAL_NOT_EXISTS_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            return mapKeysNotContain(qualifierMap);
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null;
        }
    },
    MAP_VAL_IS_NOT_NULL_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            String[] dotPathArray = getDotPathArray(getDotPath(qualifierMap),
                "MAP_VAL_IS_NULL_BY_KEY: dotPath was not set");
            if (dotPathArray.length > 1) {
                // in case it is a field of an object set to null the key does not get added to a Map,
                // so it is enough to look for Maps with the given key
                return mapKeysContain(qualifierMap);
            } else {
                // currently querying for a specific Map key with not null value is not supported,
                // it is recommended to use querying for an existing key and then filtering key:!=null
                return getMapValEqOrFail(qualifierMap, Exp::eq, "MAP_VAL_IS_NULL_BY_KEY");
            }
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null;
        }
    },
    MAP_VAL_IS_NULL_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            String[] dotPathArray = getDotPathArray(getDotPath(qualifierMap),
                "MAP_VAL_IS_NULL_BY_KEY: dotPath was not set");
            if (dotPathArray.length > 1) {
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
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null;
        }
    },
    MAP_KEYS_CONTAIN {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            return mapKeysContain(qualifierMap);
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return collectionContains(IndexCollectionType.MAPKEYS, qualifierMap);
        }
    },
    MAP_KEYS_NOT_CONTAIN {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            return mapKeysNotContain(qualifierMap);
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return collectionContains(IndexCollectionType.MAPKEYS, qualifierMap);
        }
    },
    MAP_VALUES_CONTAIN {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            return mapValuesContain(qualifierMap);
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return collectionContains(IndexCollectionType.MAPVALUES, qualifierMap);
        }
    },
    MAP_VALUES_NOT_CONTAIN {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            return mapValuesNotContain(qualifierMap);
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return collectionContains(IndexCollectionType.MAPVALUES, qualifierMap);
        }
    },
    MAP_KEYS_BETWEEN {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            validateEquality(getValue1(qualifierMap).getType(), getValue2(qualifierMap).getType(), qualifierMap,
                "MAP_KEYS_BETWEEN");

            Pair<Exp, Exp> twoValues = switch (getValue1(qualifierMap).getType()) {
                case INTEGER ->
                    Pair.of(Exp.val(getValue1(qualifierMap).toLong()), Exp.val(getValue2(qualifierMap).toLong()));
                case STRING ->
                    Pair.of(Exp.val(getValue1(qualifierMap).toString()), Exp.val(getValue2(qualifierMap).toString()));
                case LIST -> Pair.of(Exp.val((List<?>) getValue1(qualifierMap).getObject()),
                    Exp.val((List<?>) getValue2(qualifierMap).getObject()));
                case MAP -> Pair.of(Exp.val((Map<?, ?>) getValue1(qualifierMap).getObject()),
                    Exp.val((Map<?, ?>) getValue2(qualifierMap).getObject()));
                default -> throw new UnsupportedOperationException(
                    "MAP_KEYS_BETWEEN FilterExpression unsupported type: got "
                        + getValue1(qualifierMap).getClass().getSimpleName());
            };

            return Exp.gt(
                MapExp.getByKeyRange(MapReturnType.COUNT, twoValues.getFirst(), twoValues.getSecond(),
                    Exp.mapBin(getField(qualifierMap))),
                Exp.val(0));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return collectionRange(IndexCollectionType.MAPKEYS, qualifierMap);
        }
    },
    MAP_VAL_BETWEEN {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            validateEquality(getValue1(qualifierMap).getType(), getValue2(qualifierMap).getType(), qualifierMap,
                "MAP_VAL_BETWEEN");

            Pair<Exp, Exp> twoValues = switch (getValue1(qualifierMap).getType()) {
                case INTEGER ->
                    Pair.of(Exp.val(getValue1(qualifierMap).toLong()), Exp.val(getValue2(qualifierMap).toLong()));
                case STRING ->
                    Pair.of(Exp.val(getValue1(qualifierMap).toString()), Exp.val(getValue2(qualifierMap).toString()));
                case LIST -> Pair.of(Exp.val((List<?>) getValue1(qualifierMap).getObject()),
                    Exp.val((List<?>) getValue2(qualifierMap).getObject()));
                case MAP -> Pair.of(Exp.val((Map<?, ?>) getValue1(qualifierMap).getObject()),
                    Exp.val((Map<?, ?>) getValue2(qualifierMap).getObject()));
                default -> throw new UnsupportedOperationException(
                    "MAP_VAL_BETWEEN FilterExpression unsupported type: got "
                        + getValue1(qualifierMap).getClass().getSimpleName());
            };

            return Exp.gt(
                MapExp.getByValueRange(MapReturnType.COUNT, twoValues.getFirst(), twoValues.getSecond(),
                    Exp.mapBin(getField(qualifierMap))),
                Exp.val(0));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            if (getValue1(qualifierMap).getType() != INTEGER || getValue2(qualifierMap).getType() != INTEGER) {
                return null;
            }
            return collectionRange(IndexCollectionType.MAPVALUES, qualifierMap);
        }
    },
    GEO_WITHIN {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            return Exp.geoCompare(Exp.geoBin(getField(qualifierMap)), Exp.geo(getValue1(qualifierMap).toString()));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return geoWithinRadius(IndexCollectionType.DEFAULT, qualifierMap);
        }
    },
    LIST_VAL_CONTAINING {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            // boolean values are read as BoolIntValue (INTEGER ParticleType) if Value.UseBoolBin == false
            // so converting to BooleanValue to process correctly
            if (getValue1(qualifierMap) instanceof Value.BoolIntValue) {
                qualifierMap.put(VALUE1, new Value.BooleanValue((Boolean) (getValue1(qualifierMap).getObject())));
            }

            Exp value = switch (getValue1(qualifierMap).getType()) {
                case INTEGER -> Exp.val(getValue1(qualifierMap).toLong());
                case STRING -> Exp.val(getValue1(qualifierMap).toString());
                case BOOL -> Exp.val((Boolean) getValue1(qualifierMap).getObject());
                case LIST -> Exp.val((List<?>) getValue1(qualifierMap).getObject());
                case MAP -> Exp.val((Map<?, ?>) getValue1(qualifierMap).getObject());
                case ParticleType.NULL -> Exp.nil();
                default -> throw new UnsupportedOperationException(
                    "LIST_VAL_CONTAINING FilterExpression unsupported type: got " +
                        getValue1(qualifierMap).getClass().getSimpleName());
            };

            return Exp.gt(
                ListExp.getByValue(ListReturnType.COUNT, value, Exp.listBin(getField(qualifierMap))),
                Exp.val(0));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            if (getValue1(qualifierMap).getType() != STRING && getValue1(qualifierMap).getType() != INTEGER) {
                return null;
            }

            return collectionContains(IndexCollectionType.LIST, qualifierMap);
        }
    },
    LIST_VAL_NOT_CONTAINING {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            // boolean values are read as BoolIntValue (INTEGER ParticleType) if Value.UseBoolBin == false
            // so converting to BooleanValue to process correctly
            if (getValue1(qualifierMap) instanceof Value.BoolIntValue) {
                qualifierMap.put(VALUE1, new Value.BooleanValue((Boolean) (getValue1(qualifierMap).getObject())));
            }

            Exp value = switch (getValue1(qualifierMap).getType()) {
                case INTEGER -> Exp.val(getValue1(qualifierMap).toLong());
                case STRING -> Exp.val(getValue1(qualifierMap).toString());
                case BOOL -> Exp.val((Boolean) getValue1(qualifierMap).getObject());
                case LIST -> Exp.val((List<?>) getValue1(qualifierMap).getObject());
                case MAP -> Exp.val((Map<?, ?>) getValue1(qualifierMap).getObject());
                case ParticleType.NULL -> Exp.nil();
                default -> throw new UnsupportedOperationException(
                    "LIST_VAL_CONTAINING FilterExpression unsupported type: got " +
                        getValue1(qualifierMap).getClass().getSimpleName());
            };

            Exp binIsNull = Exp.not(Exp.binExists(getField(qualifierMap)));
            Exp listNotContaining = Exp.eq(
                ListExp.getByValue(ListReturnType.COUNT, value, Exp.listBin(getField(qualifierMap))),
                Exp.val(0));
            return Exp.or(binIsNull, listNotContaining);
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null; // currently not supported
        }
    },
    LIST_VAL_BETWEEN {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            validateEquality(getValue1(qualifierMap).getType(), getValue2(qualifierMap).getType(), qualifierMap,
                "LIST_VAL_BETWEEN");

            Pair<Exp, Exp> twoValues = switch (getValue1(qualifierMap).getType()) {
                case INTEGER ->
                    Pair.of(Exp.val(getValue1(qualifierMap).toLong()), Exp.val(getValue2(qualifierMap).toLong()));
                case STRING ->
                    Pair.of(Exp.val(getValue1(qualifierMap).toString()), Exp.val(getValue2(qualifierMap).toString()));
                case LIST -> Pair.of(Exp.val((List<?>) getValue1(qualifierMap).getObject()),
                    Exp.val((List<?>) getValue2(qualifierMap).getObject()));
                case MAP -> Pair.of(Exp.val((Map<?, ?>) getValue1(qualifierMap).getObject()),
                    Exp.val((Map<?, ?>) getValue2(qualifierMap).getObject()));
                default -> throw new UnsupportedOperationException(
                    "LIST_VAL_BETWEEN FilterExpression unsupported type: got "
                        + getValue1(qualifierMap).getClass().getSimpleName());
            };

            return Exp.gt(
                ListExp.getByValueRange(ListReturnType.COUNT, twoValues.getFirst(), twoValues.getSecond(),
                    Exp.listBin(getField(qualifierMap))),
                Exp.val(0));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            if (getValue1(qualifierMap).getType() != INTEGER || getValue2(qualifierMap).getType() != INTEGER) {
                return null;
            }

            return collectionRange(IndexCollectionType.LIST, qualifierMap); // both limits are inclusive
        }
    },
    LIST_VAL_GT {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            if (getValue1(qualifierMap).getType() == INTEGER) {
                if (getValue1(qualifierMap).toLong() == Long.MAX_VALUE) {
                    throw new IllegalArgumentException(
                        "LIST_VAL_GT FilterExpression unsupported value: expected [Long.MIN_VALUE.." +
                            "Long.MAX_VALUE-1]");
                }

                return Exp.gt(
                    ListExp.getByValueRange(ListReturnType.COUNT, Exp.val(getValue1(qualifierMap).toLong() + 1L),
                        null, Exp.listBin(getField(qualifierMap))),
                    Exp.val(0)
                );
            } else {
                Exp value = switch (getValue1(qualifierMap).getType()) {
                    case STRING -> Exp.val(getValue1(qualifierMap).toString());
                    case LIST -> Exp.val((List<?>) getValue1(qualifierMap).getObject());
                    case MAP -> Exp.val((Map<?, ?>) getValue1(qualifierMap).getObject());
                    default -> throw new UnsupportedOperationException(
                        "LIST_VAL_GT FilterExpression unsupported type: got "
                            + getValue1(qualifierMap).getClass().getSimpleName());
                };

                Exp rangeIncludingValue = ListExp.getByValueRange(ListReturnType.COUNT, value, null,
                    Exp.listBin(getField(qualifierMap)));
                Exp valueOnly = ListExp.getByValue(ListReturnType.COUNT, value, Exp.listBin(getField(qualifierMap)));
                return Exp.gt(Exp.sub(rangeIncludingValue, valueOnly), Exp.val(0));
            }
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            // Long.MAX_VALUE shall not be given as + 1 will cause overflow
            if (getValue1(qualifierMap).getType() != INTEGER || getValue1(qualifierMap).toLong() == Long.MAX_VALUE) {
                return null;
            }

            return Filter.range(getField(qualifierMap), IndexCollectionType.LIST,
                getValue1(qualifierMap).toLong() + 1, Long.MAX_VALUE);
        }
    },
    LIST_VAL_GTEQ {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            Exp value = switch (getValue1(qualifierMap).getType()) {
                case INTEGER -> Exp.val(getValue1(qualifierMap).toLong());
                case STRING -> Exp.val(getValue1(qualifierMap).toString());
                case LIST -> Exp.val((List<?>) getValue1(qualifierMap).getObject());
                case MAP -> Exp.val((Map<?, ?>) getValue1(qualifierMap).getObject());
                default -> throw new UnsupportedOperationException(
                    "LIST_VAL_GTEQ FilterExpression unsupported type: got "
                        + getValue1(qualifierMap).getClass().getSimpleName());
            };

            return Exp.gt(
                ListExp.getByValueRange(ListReturnType.COUNT, value, null, Exp.listBin(getField(qualifierMap))),
                Exp.val(0));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            if (getValue1(qualifierMap).getType() != INTEGER) {
                return null;
            }

            return Filter.range(getField(qualifierMap), IndexCollectionType.LIST, getValue1(qualifierMap).toLong(),
                Long.MAX_VALUE);
        }
    },
    LIST_VAL_LT {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            Exp value = switch (getValue1(qualifierMap).getType()) {
                case INTEGER -> {
                    if (getValue1(qualifierMap).toLong() == Long.MIN_VALUE) {
                        throw new UnsupportedOperationException(
                            "LIST_VAL_LT FilterExpression unsupported value: expected [Long.MIN_VALUE+1.." +
                                "Long.MAX_VALUE]");
                    }

                    yield Exp.val(getValue1(qualifierMap).toLong());
                }
                case STRING -> Exp.val(getValue1(qualifierMap).toString());
                case LIST -> Exp.val((List<?>) getValue1(qualifierMap).getObject());
                case MAP -> Exp.val((Map<?, ?>) getValue1(qualifierMap).getObject());
                default -> throw new UnsupportedOperationException(
                    "LIST_VAL_GTEQ FilterExpression unsupported type: got "
                        + getValue1(qualifierMap).getClass().getSimpleName());
            };

            return Exp.gt(
                ListExp.getByValueRange(ListReturnType.COUNT, null, value, Exp.listBin(getField(qualifierMap))),
                Exp.val(0));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            // Long.MIN_VALUE shall not be given as - 1 will cause overflow
            if (getValue1(qualifierMap).getType() != INTEGER || getValue1(qualifierMap).toLong() == Long.MIN_VALUE) {
                return null;
            }

            return Filter.range(getField(qualifierMap), IndexCollectionType.LIST, Long.MIN_VALUE,
                getValue1(qualifierMap).toLong() - 1);
        }
    },
    LIST_VAL_LTEQ {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            if (getValue1(qualifierMap).getType() == INTEGER) {
                Exp upperLimit;
                if (getValue1(qualifierMap).toLong() == Long.MAX_VALUE) {
                    upperLimit = Exp.inf();
                } else {
                    upperLimit = Exp.val(getValue1(qualifierMap).toLong() + 1L);
                }

                return Exp.gt(
                    ListExp.getByValueRange(ListReturnType.COUNT, Exp.val(Long.MIN_VALUE),
                        upperLimit, Exp.listBin(getField(qualifierMap))),
                    Exp.val(0));
            } else {
                Exp value = switch (getValue1(qualifierMap).getType()) {
                    case STRING -> Exp.val(getValue1(qualifierMap).toString());
                    case LIST -> Exp.val((List<?>) getValue1(qualifierMap).getObject());
                    case MAP -> Exp.val((Map<?, ?>) getValue1(qualifierMap).getObject());
                    default -> throw new UnsupportedOperationException(
                        "LIST_VAL_LTEQ FilterExpression unsupported type: got " +
                            getValue1(qualifierMap).getClass().getSimpleName());
                };

                Exp rangeIncludingValue = ListExp.getByValueRange(ListReturnType.COUNT, null, value,
                    Exp.listBin(getField(qualifierMap)));
                Exp valueOnly = ListExp.getByValue(ListReturnType.COUNT, value, Exp.listBin(getField(qualifierMap)));
                return Exp.gt(Exp.add(rangeIncludingValue, valueOnly), Exp.val(0));
            }
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            if (getValue1(qualifierMap).getType() != INTEGER) {
                return null;
            }

            return Filter.range(getField(qualifierMap), IndexCollectionType.LIST, Long.MIN_VALUE,
                getValue1(qualifierMap).toLong());
        }
    }, IS_NOT_NULL {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            return Exp.binExists(getField(qualifierMap));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null;
        }
    }, IS_NULL {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            return Exp.not(Exp.binExists(getField(qualifierMap))); // with value set to null a bin becomes non-existing
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null;
        }
    }, NOT_NULL {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            return Exp.binExists(getField(qualifierMap)); // if a bin exists its value is not null
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null;
        }
    };

    private static Exp processMetadataFieldInOrNot(Map<String, Object> qualifierMap, boolean notIn) {
        FilterOperation filterOperation = notIn ? NOTEQ : EQ;
        Object value1 = getValue1Object(qualifierMap);

        Collection<Long> listOfLongs;
        try {
            listOfLongs = (Collection<Long>) value1; // previously validated
        } catch (Exception e) {
            String operation = notIn ? "NOT_IN" : "IN";
            throw new IllegalStateException("FilterOperation." + operation + " metadata query: expecting value1 with " +
                "type List<Long>");
        }
        Exp[] listElementsExp = listOfLongs.stream().map(item ->
            new Qualifier(
                new QualifierBuilder()
                    .setMetadataField(getMetadataField(qualifierMap))
                    .setFilterOperation(filterOperation)
                    .setValue1AsObj(item)
            ).toFilterExp()
        ).toArray(Exp[]::new);

        return notIn ? Exp.and(listElementsExp) : Exp.or(listElementsExp);
    }

    private static Exp processMetadataFieldIn(Map<String, Object> qualifierMap) {
        return processMetadataFieldInOrNot(qualifierMap, false);
    }

    private static Exp processMetadataFieldNotIn(Map<String, Object> qualifierMap) {
        return processMetadataFieldInOrNot(qualifierMap, true);
    }

    private static Value getValue1AsCollectionOrFail(Map<String, Object> qualifierMap) {
        Value value1 = getValue1(qualifierMap);
        String errMsg = "FilterOperation.IN expects argument with type Collection, instead got: " +
            value1.getObject().getClass().getSimpleName();
        if (value1.getType() != LIST || !(value1.getObject() instanceof Collection<?>)) {
            throw new IllegalArgumentException(errMsg);
        }
        return value1;
    }

    /**
     * If metadata field has a value and regular field hasn't got value, build an Exp to query by metadata using
     * information set in the given qualifier map.
     *
     * @param qualifierMap Map with qualifier data
     * @return Optional with the Exp or Optional.empty()
     */
    private static Optional<Exp> getMetadataExp(Map<String, Object> qualifierMap) {
        CriteriaDefinition.AerospikeMetadata metadataField = getMetadataField(qualifierMap);
        String field = getField(qualifierMap);

        if (metadataField != null && (field == null || field.isEmpty())) {
            FilterOperation operation = getOperation(qualifierMap);
            switch (operation) {
                case EQ, NOTEQ, LT, LTEQ, GT, GTEQ -> {
                    BinaryOperator<Exp> operationFunction = mapOperation(operation);
                    return Optional.of(
                        operationFunction.apply(
                            mapMetadataExp(metadataField),
                            Exp.val(getValue1AsLongOrFail(getValue1Object(qualifierMap)))
                        )
                    );
                }
                case BETWEEN -> {
                    Exp metadata = mapMetadataExp(metadataField);
                    Exp value1 = Exp.val(getValue1(qualifierMap).toLong());
                    Exp value2 = Exp.val(getValue2(qualifierMap).toLong());
                    return Optional.of(Exp.and(Exp.ge(metadata, value1), Exp.lt(metadata, value2)));
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

    // expecting value1 always be of type Long
    private static Long getValue1AsLongOrFail(Object value1) {
        try {
            return (Long) value1;
        } catch (Exception e) {
            throw new IllegalArgumentException("Expecting value1 to be of type Long");
        }
    }

    private static Exp mapMetadataExp(CriteriaDefinition.AerospikeMetadata metadataField) {
        return switch (metadataField) {
            case SINCE_UPDATE_TIME -> Exp.sinceUpdate();
            case LAST_UPDATE_TIME -> Exp.lastUpdate();
            case VOID_TIME -> Exp.voidTime();
            case TTL -> Exp.ttl();
            case RECORD_SIZE_ON_DISK -> Exp.deviceSize();
            case RECORD_SIZE_IN_MEMORY -> Exp.memorySize();
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

    private static Exp mapKeysNotContain(Map<String, Object> qualifierMap) {
        String errMsg = "MAP_KEYS_NOT_CONTAIN FilterExpression unsupported type: got " +
            getValue1(qualifierMap).getClass().getSimpleName();
        Exp mapKeysNotContain = mapKeysCount(qualifierMap, Exp::eq, errMsg);
        Exp binDoesNotExist = Exp.not(Exp.binExists(getField(qualifierMap)));
        return Exp.or(binDoesNotExist, mapKeysNotContain);
    }

    private static Exp mapKeysContain(Map<String, Object> qualifierMap) {
        String errMsg = "MAP_KEYS_CONTAIN FilterExpression unsupported type: got " +
            getValue1(qualifierMap).getClass().getSimpleName();
        return mapKeysCount(qualifierMap, Exp::gt, errMsg);
    }

    private static Exp mapValuesNotContain(Map<String, Object> qualifierMap) {
        String errMsg = "MAP_VALUES_NOT_CONTAIN FilterExpression unsupported type: got " +
            getValue1(qualifierMap).getClass().getSimpleName();
        Exp binDoesNotExist = Exp.not(Exp.binExists(getField(qualifierMap)));
        Exp mapValuesNotContain = mapValuesCount(qualifierMap, Exp::eq, errMsg);
        return Exp.or(binDoesNotExist, mapValuesNotContain);
    }

    private static Exp mapValuesContain(Map<String, Object> qualifierMap) {
        String errMsg = "MAP_VALUES_CONTAIN FilterExpression unsupported type: got " +
            getValue1(qualifierMap).getClass().getSimpleName();
        return mapValuesCount(qualifierMap, Exp::gt, errMsg);
    }

    private static Exp getValue1Exp(Map<String, Object> qualifierMap, String errMsg) {
        return switch (getValue1(qualifierMap).getType()) {
            case INTEGER -> Exp.val(getValue1(qualifierMap).toLong());
            case STRING -> Exp.val(getValue1(qualifierMap).toString());
            case LIST -> Exp.val((List<?>) getValue1(qualifierMap).getObject());
            case MAP -> Exp.val((Map<?, ?>) getValue1(qualifierMap).getObject());
            case NULL -> Exp.nil();
            default -> throw new UnsupportedOperationException(errMsg);
        };
    }

    // operator is Exp::gt to query for mapKeysContain or Exp::eq to query for mapKeysNotContain
    private static Exp mapKeysCount(Map<String, Object> qualifierMap, BinaryOperator<Exp> operator, String errMsg) {
        Exp value = getValue1Exp(qualifierMap, errMsg);
        return operator.apply(
            MapExp.getByKey(MapReturnType.COUNT, Exp.Type.INT, value, Exp.mapBin(getField(qualifierMap))),
            Exp.val(0));
    }

    // operator is Exp::gt to query for mapValuesContain or Exp::eq to query for mapValuesNotContain
    private static Exp mapValuesCount(Map<String, Object> qualifierMap, BinaryOperator<Exp> operator, String errMsg) {
        Exp value = getValue1Exp(qualifierMap, errMsg);
        return operator.apply(
            MapExp.getByValue(MapReturnType.COUNT, value, Exp.mapBin(getField(qualifierMap))),
            Exp.val(0));
    }

    private static void validateEquality(int type1, int type2, Map<String, Object> qualifierMap, String opName) {
        if (type1 != type2) {
            throw new IllegalArgumentException(opName + ": expected both parameters to have the same "
                + "type, instead got " + getValue1(qualifierMap).getClass().getSimpleName() + " and " +
                getValue2(qualifierMap).getClass().getSimpleName());
        }
    }

    /**
     * FilterOperations that require both sIndexFilter and FilterExpression
     */
    public static final List<FilterOperation> dualFilterOperations = Arrays.asList(
        MAP_VAL_EQ_BY_KEY, MAP_VAL_GT_BY_KEY, MAP_VAL_GTEQ_BY_KEY, MAP_VAL_LT_BY_KEY, MAP_VAL_LTEQ_BY_KEY,
        MAP_VAL_BETWEEN_BY_KEY
    );

    private static Exp getFilterExpMapValOrFail(Map<String, Object> qualifierMap, BinaryOperator<Exp> operator,
                                                String opName) {
        String[] dotPathArr = getDotPathArray(getDotPath(qualifierMap),
            opName + " filter expression: dotPath has not been set");

        return switch (getValue1(qualifierMap).getType()) {
            case INTEGER -> operator.apply(getMapExp(qualifierMap, dotPathArr, Exp.Type.INT),
                Exp.val(getValue1(qualifierMap).toLong()));
            case STRING -> operator.apply(getMapExp(qualifierMap, dotPathArr, Exp.Type.STRING),
                Exp.val(getValue1(qualifierMap).toString()));
            case LIST -> operator.apply(getMapExp(qualifierMap, dotPathArr, Exp.Type.LIST),
                Exp.val((List<?>) getValue1(qualifierMap).getObject()));
            case MAP -> operator.apply(getMapExp(qualifierMap, dotPathArr, Exp.Type.MAP),
                Exp.val((Map<?, ?>) getValue1(qualifierMap).getObject()));
            default -> throw new UnsupportedOperationException(
                opName + " FilterExpression unsupported type: " + getValue1(qualifierMap).getClass().getSimpleName());
        };
    }

    private static Exp getMapExp(Map<String, Object> qualifierMap, String[] dotPathArr, Exp.Type expType) {
        // VALUE2 contains key (field name)
        // currently the only Map keys supported are Strings
        if (dotPathArr.length > 2) {
            return MapExp.getByKey(MapReturnType.VALUE, expType, Exp.val(getValue2(qualifierMap).toString()),
                Exp.mapBin(getField(qualifierMap)), dotPathToCtxMapKeys(dotPathArr));
        } else {
            return MapExp.getByKey(MapReturnType.VALUE, expType, Exp.val(getValue2(qualifierMap).toString()),
                Exp.mapBin(getField(qualifierMap)));
        }
    }

    private static Exp getFilterExpMapValEqOrFail(Map<String, Object> qualifierMap, BinaryOperator<Exp> operator) {
        return getMapValEqOrFail(qualifierMap, operator, "MAP_VAL_EQ_BY_KEY");
    }

    private static Exp getFilterExpMapValNotEqOrFail(Map<String, Object> qualifierMap, BinaryOperator<Exp> operator) {
        return getMapValEqOrFail(qualifierMap, operator, "MAP_VAL_NOTEQ_BY_KEY");
    }

    private static Exp getMapValEqOrFail(Map<String, Object> qualifierMap, BinaryOperator<Exp> operator,
                                         String opName) {
        String[] dotPathArr = getDotPathArray(getDotPath(qualifierMap),
            opName + " filter expression: dotPath has not been set");
        final boolean useCtx = dotPathArr.length > 2;

        // boolean values are read as BoolIntValue (INTEGER ParticleType) if Value.UseBoolBin == false
        // so converting to BooleanValue to process correctly
        if (getValue1(qualifierMap) instanceof Value.BoolIntValue) {
            qualifierMap.put(VALUE1, new Value.BooleanValue((Boolean) getValue1(qualifierMap).getObject()));
        }

        Value value1 = getValue1(qualifierMap);
        return switch (value1.getType()) {
            case INTEGER -> getMapValEqExp(qualifierMap, Exp.Type.INT, value1.toLong(), dotPathArr, operator,
                useCtx);
            case STRING -> {
                if (ignoreCase(qualifierMap)) {
                    throw new UnsupportedOperationException(
                        opName + " FilterExpression: case insensitive comparison is not supported");
                }
                yield getMapValEqExp(qualifierMap, Exp.Type.STRING, value1.toString(), dotPathArr, operator,
                    useCtx);
            }
            case BOOL -> getMapValEqExp(qualifierMap, Exp.Type.BOOL, value1.getObject(), dotPathArr, operator,
                useCtx);
            case LIST -> getMapValEqExp(qualifierMap, Exp.Type.LIST, value1.getObject(), dotPathArr, operator,
                useCtx);
            case MAP -> getMapValEqExp(qualifierMap, Exp.Type.MAP, value1.getObject(), dotPathArr, operator, useCtx);
            default -> throw new UnsupportedOperationException(
                opName + " FilterExpression unsupported type: " + value1.getClass().getSimpleName());
        };
    }

    private static Exp getMapValEqExp(Map<String, Object> qualifierMap, Exp.Type expType, Object value,
                                      String[] dotPathArr,
                                      BinaryOperator<Exp> operator, boolean useCtx) {
        Exp mapExp = getMapValEq(qualifierMap, expType, dotPathArr, useCtx);
        return operator.apply(mapExp, toExp(value));
    }

    private static Exp getMapValEq(Map<String, Object> qualifierMap, Exp.Type expType, String[] dotPathArr,
                                   boolean useCtx) {
        if (useCtx) {
            return MapExp.getByKey(MapReturnType.VALUE, expType,
                Exp.val(getValue2(qualifierMap).toString()), // VALUE2 contains key (field name)
                Exp.mapBin(getField(qualifierMap)), dotPathToCtxMapKeys(dotPathArr));
        } else {
            return MapExp.getByKey(MapReturnType.VALUE, expType,
                Exp.val(getValue2(qualifierMap).toString()),
                Exp.mapBin(getField(qualifierMap)));
        }
    }

    private static Exp getFilterExp(Exp exp, String field,
                                    BinaryOperator<Exp> operator, Function<String, Exp> binExp) {
        return operator.apply(binExp.apply(field), exp);
    }

    private static String[] getDotPathArray(String dotPath, String errMsg) {
        if (StringUtils.hasLength(dotPath)) {
            return dotPath.split("\\.");
        } else {
            throw new IllegalStateException(errMsg);
        }
    }

    private static CTX[] dotPathToCtxMapKeys(String[] dotPathArray) {
        return Arrays.stream(dotPathArray).map(str -> CTX.mapKey(Value.get(str)))
            .skip(1) // first element is bin name
            .limit(dotPathArray.length - 2L) // last element is the key we already have
            .toArray(CTX[]::new);
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

    protected static String getField(Map<String, Object> qualifierMap) {
        return (String) qualifierMap.get(FIELD);
    }

    protected static CriteriaDefinition.AerospikeMetadata getMetadataField(Map<String, Object> qualifierMap) {
        return (CriteriaDefinition.AerospikeMetadata) qualifierMap.get(METADATA_FIELD);
    }

    protected static FilterOperation getOperation(Map<String, Object> qualifierMap) {
        return (FilterOperation) qualifierMap.get(OPERATION);
    }

    protected static Boolean ignoreCase(Map<String, Object> qualifierMap) {
        return (Boolean) qualifierMap.getOrDefault(IGNORE_CASE, false);
    }

    protected static int regexFlags(Map<String, Object> qualifierMap) {
        return ignoreCase(qualifierMap) ? RegexFlag.ICASE : RegexFlag.NONE;
    }

    protected static Value getValue1(Map<String, Object> qualifierMap) {
        return Value.get(qualifierMap.get(VALUE1));
    }

    protected static Object getValue1Object(Map<String, Object> qualifierMap) {
        return qualifierMap.get(VALUE1);
    }

    protected static Value getValue2(Map<String, Object> qualifierMap) {
        return Value.get(qualifierMap.get(VALUE2));
    }

    protected static Value getValue3(Map<String, Object> qualifierMap) {
        return (Value) qualifierMap.get(VALUE3);
    }

    protected static String getDotPath(Map<String, Object> qualifierMap) {
        return (String) qualifierMap.get(DOT_PATH);
    }

    protected static MappingAerospikeConverter getConverter(Map<String, Object> qualifierMap) {
        return (MappingAerospikeConverter) qualifierMap.get(CONVERTER);
    }

    public abstract Exp filterExp(Map<String, Object> qualifierMap);

    public abstract Filter sIndexFilter(Map<String, Object> qualifierMap);

    protected Filter collectionContains(IndexCollectionType collectionType, Map<String, Object> qualifierMap) {
        Value val = getValue1(qualifierMap);
        int valType = val.getType();
        return switch (valType) {
            case INTEGER -> Filter.contains(getField(qualifierMap), collectionType, val.toLong());
            case STRING -> Filter.contains(getField(qualifierMap), collectionType, val.toString());
            default -> null;
        };
    }

    protected Filter collectionRange(IndexCollectionType collectionType, Map<String, Object> qualifierMap) {
        return Filter.range(getField(qualifierMap), collectionType, getValue1(qualifierMap).toLong(),
            getValue2(qualifierMap).toLong());
    }

    @SuppressWarnings("SameParameterValue")
    protected Filter geoWithinRadius(IndexCollectionType collectionType, Map<String, Object> qualifierMap) {
        return Filter.geoContains(getField(qualifierMap), getValue1(qualifierMap).toString());
    }
}
