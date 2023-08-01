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
import org.springframework.data.aerospike.query.Qualifier.QualifierBuilder;
import org.springframework.data.util.Pair;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import static com.aerospike.client.command.ParticleType.BOOL;
import static com.aerospike.client.command.ParticleType.INTEGER;
import static com.aerospike.client.command.ParticleType.JBLOB;
import static com.aerospike.client.command.ParticleType.LIST;
import static com.aerospike.client.command.ParticleType.MAP;
import static com.aerospike.client.command.ParticleType.NULL;
import static com.aerospike.client.command.ParticleType.STRING;
import static org.springframework.data.aerospike.query.Qualifier.CONVERTER;
import static org.springframework.data.aerospike.query.Qualifier.DOT_PATH;
import static org.springframework.data.aerospike.query.Qualifier.FIELD;
import static org.springframework.data.aerospike.query.Qualifier.IGNORE_CASE;
import static org.springframework.data.aerospike.query.Qualifier.QUALIFIERS;
import static org.springframework.data.aerospike.query.Qualifier.QualifierRegexpBuilder;
import static org.springframework.data.aerospike.query.Qualifier.VALUE1;
import static org.springframework.data.aerospike.query.Qualifier.VALUE2;
import static org.springframework.data.aerospike.query.Qualifier.VALUE3;

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
            Value val = getValue1(qualifierMap);
            String errMsg = "FilterOperation.IN expects argument with type Collection, instead got: " +
                val.getObject().getClass().getSimpleName();
            if (val.getType() != LIST && val.getType() != JBLOB) { // some Collection classes come as JBLOB
                throw new IllegalArgumentException(errMsg);
            } else {
                if (!(val.getObject() instanceof Collection<?>)) {
                    throw new IllegalArgumentException(errMsg);
                }
            }

            Collection<?> collection = (Collection<?>) val.getObject();
            Exp[] listElementsExp = collection.stream().map(item ->
                new Qualifier(
                    new QualifierBuilder()
                        .setField(getField(qualifierMap))
                        .setFilterOperation(FilterOperation.EQ)
                        .setValue1(Value.get(item))
                ).toFilterExp()
            ).toArray(Exp[]::new);

            return Exp.or(listElementsExp);
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
            Value val = getValue1(qualifierMap);
            String errMsg = "FilterOperation.NOT_IN expects argument with type Collection, instead got: " +
                val.getObject().getClass().getSimpleName();
            if (val.getType() != LIST && val.getType() != JBLOB) { // some Collection classes come as JBLOB
                throw new IllegalArgumentException(errMsg);
            } else {
                if (!(val.getObject() instanceof Collection<?>)) {
                    throw new IllegalArgumentException(errMsg);
                }
            }

            Collection<?> collection = (Collection<?>) val.getObject();
            Exp[] listElementsExp = collection.stream().map(item ->
                new Qualifier(
                    new QualifierBuilder()
                        .setField(getField(qualifierMap))
                        .setFilterOperation(FilterOperation.NOTEQ)
                        .setValue1(Value.get(item))
                ).toFilterExp()
            ).toArray(Exp[]::new);

            return Exp.and(listElementsExp);
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null;
        }
    },
    EQ {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            Value value = getValue1(qualifierMap);
            return switch (value.getType()) {
                case INTEGER -> Exp.eq(Exp.intBin(getField(qualifierMap)), Exp.val(value.toLong()));
                case STRING -> {
                    if (ignoreCase(qualifierMap)) {
                        String equalsRegexp =
                            QualifierRegexpBuilder.getStringEquals(getValue1(qualifierMap).toString());
                        yield Exp.regexCompare(equalsRegexp, RegexFlag.ICASE, Exp.stringBin(getField(qualifierMap)));
                    } else {
                        yield Exp.eq(Exp.stringBin(getField(qualifierMap)), Exp.val(value.toString()));
                    }
                }
                case BOOL -> Exp.eq(Exp.boolBin(getField(qualifierMap)), Exp.val((Boolean) value.getObject()));
                case JBLOB -> getFilterExp(getConverter(qualifierMap), value, getField(qualifierMap), Exp::eq);
                case MAP -> getFilterExp(Exp.val(getConvertedMap(qualifierMap, value)), getField(qualifierMap), Exp::eq,
                    Exp::mapBin);
                case LIST ->
                    getFilterExp(Exp.val((List<?>) value.getObject()), getField(qualifierMap), Exp::eq, Exp::listBin);
                default -> throw new IllegalArgumentException("EQ FilterExpression unsupported particle type: " +
                    value.getClass().getSimpleName());
            };
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
                case JBLOB -> getFilterExp(getConverter(qualifierMap), value, getField(qualifierMap), Exp::ne);
                case MAP -> getFilterExp(Exp.val(getConvertedMap(qualifierMap, value)), getField(qualifierMap), Exp::ne,
                    Exp::mapBin);
                case LIST ->
                    getFilterExp(Exp.val((List<?>) value.getObject()), getField(qualifierMap), Exp::ne, Exp::listBin);
                default -> throw new IllegalArgumentException("NOTEQ FilterExpression unsupported particle type: " +
                    value.getClass().getSimpleName());
            };
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> qualifierMap) {
            return null; // not supported in the secondary index filter
        }
    },
    GT {
        @Override
        public Exp filterExp(Map<String, Object> qualifierMap) {
            Value value = getValue1(qualifierMap);
            return switch (value.getType()) {
                case INTEGER -> Exp.gt(Exp.intBin(getField(qualifierMap)), Exp.val(getValue1(qualifierMap).toLong()));
                case STRING ->
                    Exp.gt(Exp.stringBin(getField(qualifierMap)), Exp.val(getValue1(qualifierMap).toString()));
                case JBLOB -> getFilterExp(getConverter(qualifierMap), value, getField(qualifierMap), Exp::gt);
                case MAP -> getFilterExp(Exp.val(getConvertedMap(qualifierMap, value)), getField(qualifierMap), Exp::gt,
                    Exp::mapBin);
                case LIST ->
                    getFilterExp(Exp.val((List<?>) value.getObject()), getField(qualifierMap), Exp::gt, Exp::listBin);
                default -> throw new IllegalArgumentException("GT FilterExpression unsupported particle type: " +
                    value.getClass().getSimpleName());
            };
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
            Value value = getValue1(qualifierMap);
            return switch (value.getType()) {
                case INTEGER -> Exp.ge(Exp.intBin(getField(qualifierMap)), Exp.val(getValue1(qualifierMap).toLong()));
                case STRING ->
                    Exp.ge(Exp.stringBin(getField(qualifierMap)), Exp.val(getValue1(qualifierMap).toString()));
                case JBLOB -> getFilterExp(getConverter(qualifierMap), value, getField(qualifierMap), Exp::ge);
                case MAP -> getFilterExp(Exp.val(getConvertedMap(qualifierMap, value)), getField(qualifierMap), Exp::ge,
                    Exp::mapBin);
                case LIST ->
                    getFilterExp(Exp.val((List<?>) value.getObject()), getField(qualifierMap), Exp::ge, Exp::listBin);
                default -> throw new IllegalArgumentException("GTEQ FilterExpression unsupported particle type: " +
                    value.getClass().getSimpleName());
            };
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
            Value value = getValue1(qualifierMap);
            return switch (value.getType()) {
                case INTEGER -> Exp.lt(Exp.intBin(getField(qualifierMap)), Exp.val(getValue1(qualifierMap).toLong()));
                case STRING ->
                    Exp.lt(Exp.stringBin(getField(qualifierMap)), Exp.val(getValue1(qualifierMap).toString()));
                case JBLOB -> getFilterExp(getConverter(qualifierMap), value, getField(qualifierMap), Exp::lt);
                case MAP -> getFilterExp(Exp.val(getConvertedMap(qualifierMap, value)), getField(qualifierMap), Exp::lt,
                    Exp::mapBin);
                case LIST ->
                    getFilterExp(Exp.val((List<?>) value.getObject()), getField(qualifierMap), Exp::lt, Exp::listBin);
                default -> throw new IllegalArgumentException("LT FilterExpression unsupported particle type: " +
                    value.getClass().getSimpleName());
            };
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
            Value value = getValue1(qualifierMap);
            return switch (value.getType()) {
                case INTEGER -> Exp.le(Exp.intBin(getField(qualifierMap)), Exp.val(getValue1(qualifierMap).toLong()));
                case STRING ->
                    Exp.le(Exp.stringBin(getField(qualifierMap)), Exp.val(getValue1(qualifierMap).toString()));
                case JBLOB -> getFilterExp(getConverter(qualifierMap), value, getField(qualifierMap), Exp::le);
                case MAP -> getFilterExp(Exp.val(getConvertedMap(qualifierMap, value)), getField(qualifierMap), Exp::le,
                    Exp::mapBin);
                case LIST ->
                    getFilterExp(Exp.val((List<?>) value.getObject()), getField(qualifierMap), Exp::le, Exp::listBin);
                default -> throw new IllegalArgumentException("LTEQ FilterExpression unsupported particle type: " +
                    value.getClass().getSimpleName());
            };
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

            return switch (getValue1(qualifierMap).getType()) {
                case INTEGER -> Exp.and(
                    Exp.ge(Exp.intBin(getField(qualifierMap)), Exp.val(getValue1(qualifierMap).toLong())),
                    Exp.lt(Exp.intBin(getField(qualifierMap)), Exp.val(getValue2(qualifierMap).toLong()))
                );
                case STRING -> Exp.and(
                    Exp.ge(Exp.stringBin(getField(qualifierMap)), Exp.val(getValue1(qualifierMap).toString())),
                    Exp.lt(Exp.stringBin(getField(qualifierMap)), Exp.val(getValue2(qualifierMap).toString()))
                );
                case JBLOB -> Exp.and(
                    getFilterExp(getConverter(qualifierMap), getValue1(qualifierMap), getField(qualifierMap), Exp::ge),
                    getFilterExp(getConverter(qualifierMap), getValue2(qualifierMap), getField(qualifierMap), Exp::lt)
                );
                case MAP -> Exp.and(
                    getFilterExp(Exp.val(getConvertedMap(qualifierMap, FilterOperation::getValue1)),
                        getField(qualifierMap), Exp::ge, Exp::mapBin),
                    getFilterExp(Exp.val(getConvertedMap(qualifierMap, FilterOperation::getValue2)),
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
            };
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
                case JBLOB -> {
                    Object convertedValue1 = getConvertedValue(qualifierMap, FilterOperation::getValue1);
                    Object convertedValue3 = getConvertedValue(qualifierMap, FilterOperation::getValue3);
                    if (convertedValue1 instanceof List<?>) {
                        // Collection comes as JBLOB
                        value1 = Exp.val((List<?>) convertedValue1);
                        value2 = Exp.val((List<?>) convertedValue3);
                        type = Exp.Type.LIST;
                    } else {
                        // custom objects are converted into Maps
                        value1 = Exp.val((Map<?, ?>) convertedValue1);
                        value2 = Exp.val((Map<?, ?>) convertedValue3);
                        type = Exp.Type.MAP;
                    }
                }
                case LIST -> {
                    value1 = Exp.val((List<?>) getValue1(qualifierMap).getObject());
                    value2 = Exp.val((List<?>) getValue3(qualifierMap).getObject());
                    type = Exp.Type.LIST;
                }
                case MAP -> {
                    value1 = Exp.val(getConvertedMap(qualifierMap, FilterOperation::getValue1));
                    value2 = Exp.val(getConvertedMap(qualifierMap, FilterOperation::getValue3));
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
                case JBLOB ->
                    getTwoConvertedValuesExp(qualifierMap, FilterOperation::getValue1, FilterOperation::getValue2);
                case LIST -> Pair.of(Exp.val((List<?>) getValue1(qualifierMap).getObject()),
                    Exp.val((List<?>) getValue2(qualifierMap).getObject()));
                case MAP -> Pair.of(Exp.val(getConvertedMap(qualifierMap, FilterOperation::getValue1)),
                    Exp.val(getConvertedMap(qualifierMap, FilterOperation::getValue2)));
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
                case JBLOB ->
                    getTwoConvertedValuesExp(qualifierMap, FilterOperation::getValue1, FilterOperation::getValue2);
                case LIST -> Pair.of(Exp.val((List<?>) getValue1(qualifierMap).getObject()),
                    Exp.val((List<?>) getValue2(qualifierMap).getObject()));
                case MAP -> Pair.of(Exp.val(getConvertedMap(qualifierMap, FilterOperation::getValue1)),
                    Exp.val(getConvertedMap(qualifierMap, FilterOperation::getValue2)));
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
                case JBLOB -> getConvertedValue1Exp(qualifierMap);
                case LIST -> Exp.val((List<?>) getValue1(qualifierMap).getObject());
                case MAP -> Exp.val(getConvertedMap(qualifierMap, FilterOperation::getValue1));
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
                case JBLOB -> getConvertedValue1Exp(qualifierMap);
                case LIST -> Exp.val((List<?>) getValue1(qualifierMap).getObject());
                case MAP -> Exp.val(getConvertedMap(qualifierMap, FilterOperation::getValue1));
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
                case JBLOB ->
                    getTwoConvertedValuesExp(qualifierMap, FilterOperation::getValue1, FilterOperation::getValue2);
                case LIST -> Pair.of(Exp.val((List<?>) getValue1(qualifierMap).getObject()),
                    Exp.val((List<?>) getValue2(qualifierMap).getObject()));
                case MAP -> Pair.of(Exp.val(getConvertedMap(qualifierMap, FilterOperation::getValue1)),
                    Exp.val(getConvertedMap(qualifierMap, FilterOperation::getValue2)));
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
                    case JBLOB -> FilterOperation.getConvertedValue1Exp(qualifierMap);
                    case LIST -> Exp.val((List<?>) getValue1(qualifierMap).getObject());
                    case MAP -> Exp.val(getConvertedMap(qualifierMap, FilterOperation::getValue1));
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
                case JBLOB -> FilterOperation.getConvertedValue1Exp(qualifierMap);
                case LIST -> Exp.val((List<?>) getValue1(qualifierMap).getObject());
                case MAP -> Exp.val(getConvertedMap(qualifierMap, FilterOperation::getValue1));
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
                case JBLOB -> FilterOperation.getConvertedValue1Exp(qualifierMap);
                case LIST -> Exp.val((List<?>) getValue1(qualifierMap).getObject());
                case MAP -> Exp.val(getConvertedMap(qualifierMap, FilterOperation::getValue1));
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
                    case JBLOB -> FilterOperation.getConvertedValue1Exp(qualifierMap);
                    case LIST -> Exp.val((List<?>) getValue1(qualifierMap).getObject());
                    case MAP -> Exp.val(getConvertedMap(qualifierMap, FilterOperation::getValue1));
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

    private static Exp mapKeysNotContain(Map<String, Object> qualifierMap) {
        String errMsg = "MAP_KEYS_NOT_CONTAIN FilterExpression unsupported type: got " +
            getValue1(qualifierMap).getClass().getSimpleName();
        return mapKeysCount(qualifierMap, Exp::eq, errMsg);
    }

    private static Exp mapKeysContain(Map<String, Object> qualifierMap) {
        String errMsg = "MAP_KEYS_CONTAIN FilterExpression unsupported type: got " +
            getValue1(qualifierMap).getClass().getSimpleName();
        return mapKeysCount(qualifierMap, Exp::gt, errMsg);
    }

    private static Exp mapKeysCount(Map<String, Object> qualifierMap, BinaryOperator<Exp> operator, String errMsg) {
        Exp value = getValue1Exp(qualifierMap, errMsg);
        return operator.apply(
            MapExp.getByKey(MapReturnType.COUNT, Exp.Type.INT, value, Exp.mapBin(getField(qualifierMap))),
            Exp.val(0));
    }

    private static Exp mapValuesNotContain(Map<String, Object> qualifierMap) {
        String errMsg = "MAP_VALUES_NOT_CONTAIN FilterExpression unsupported type: got " +
            getValue1(qualifierMap).getClass().getSimpleName();
        return mapValuesCount(qualifierMap, Exp::eq, errMsg);
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
            case JBLOB -> getConvertedValue1Exp(qualifierMap);
            case LIST -> Exp.val((List<?>) getValue1(qualifierMap).getObject());
            case MAP -> Exp.val(getConvertedMap(qualifierMap, FilterOperation::getValue1));
            case NULL -> Exp.nil();
            default -> throw new UnsupportedOperationException(errMsg);
        };
    }

    private static Exp mapValuesCount(Map<String, Object> qualifierMap, BinaryOperator<Exp> operator, String errMsg) {
        Exp value = getValue1Exp(qualifierMap, errMsg);
        return operator.apply(
            MapExp.getByValue(MapReturnType.COUNT, value, Exp.mapBin(getField(qualifierMap))),
            Exp.val(0));
    }

    private static Exp getConvertedValue1Exp(Map<String, Object> qualifierMap) {
        Object convertedValue = getConvertedValue(qualifierMap, FilterOperation::getValue1);
        Exp exp;
        if (convertedValue instanceof List<?>) {
            // Collection comes as JBLOB
            exp = Exp.val((List<?>) convertedValue);
        } else {
            // custom objects are converted into Maps
            exp = Exp.val((Map<?, ?>) convertedValue);
        }
        return exp;
    }

    private static Pair<Exp, Exp> getTwoConvertedValuesExp(Map<String, Object> qualifierMap,
                                                           Function<Map<String, Object>, Value> getValueFunc1,
                                                           Function<Map<String, Object>, Value> getValueFunc2) {
        Object convertedValue1 = getConvertedValue(qualifierMap, getValueFunc1);
        Object convertedValue2 = getConvertedValue(qualifierMap, getValueFunc2);
        Exp exp1, exp2;
        if (convertedValue1 instanceof List<?>) {
            // Collection comes as JBLOB
            exp1 = Exp.val((List<?>) convertedValue1);
            exp2 = Exp.val((List<?>) convertedValue2);
        } else {
            // custom objects are converted into Maps
            exp1 = Exp.val((Map<?, ?>) convertedValue1);
            exp2 = Exp.val((Map<?, ?>) convertedValue2);
        }
        return Pair.of(exp1, exp2);
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
            case JBLOB -> {
                Object convertedValue = getConvertedValue(qualifierMap, FilterOperation::getValue1);
                if (convertedValue instanceof List<?>) {
                    // Collection comes as JBLOB
                    yield operator.apply(getMapExp(qualifierMap, dotPathArr, Exp.Type.LIST),
                        Exp.val((List<?>) convertedValue));
                } else {
                    // custom objects are converted into Maps
                    yield operator.apply(getMapExp(qualifierMap, dotPathArr, Exp.Type.MAP),
                        Exp.val((Map<?, ?>) convertedValue));
                }
            }
            case LIST -> operator.apply(getMapExp(qualifierMap, dotPathArr, Exp.Type.LIST),
                Exp.val((List<?>) getValue1(qualifierMap).getObject()));
            case MAP -> operator.apply(getMapExp(qualifierMap, dotPathArr, Exp.Type.MAP),
                Exp.val(getConvertedMap(qualifierMap, FilterOperation::getValue1)));
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
            case JBLOB -> {
                Object convertedValue = getConvertedValue(qualifierMap, value1);
                // Collection comes as JBLOB, custom objects are converted into Maps
                Exp.Type expType = convertedValue instanceof List<?> ? Exp.Type.LIST : Exp.Type.MAP;

                yield getMapValEqExp(qualifierMap, expType, convertedValue, dotPathArr, operator,
                    useCtx);
            }
            case LIST -> getMapValEqExp(qualifierMap, Exp.Type.LIST, value1.getObject(), dotPathArr, operator,
                useCtx);
            case MAP -> getMapValEqExp(qualifierMap, Exp.Type.MAP, Exp.val(getConvertedMap(qualifierMap, value1)),
                dotPathArr, operator, useCtx);
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

    private static Object getConvertedValue(Map<String, Object> qualifierMap,
                                            Function<Map<String, Object>, Value> function) {
        return getConverter(qualifierMap).toWritableValue(
            function.apply(qualifierMap).getObject(), TypeInformation.of(function.apply(qualifierMap).getObject()
                .getClass())
        );
    }

    private static Object getConvertedValue(Map<String, Object> qualifierMap, Value value) {
        return getConverter(qualifierMap).toWritableValue(
            value.getObject(), TypeInformation.of(value.getObject().getClass())
        );
    }

    private static Map<String, Object> getConvertedMap(Map<String, Object> qualifierMap,
                                                       Function<Map<String, Object>, Value> function) {
        Map<Object, Object> sourceMap = (Map<Object, Object>) function.apply(qualifierMap).getObject();
        return getConverter(qualifierMap).toWritableMap(sourceMap);
    }

    private static Map<String, Object> getConvertedMap(Map<String, Object> qualifierMap, Value value) {
        Map<Object, Object> sourceMap = (Map<Object, Object>) value.getObject();
        return getConverter(qualifierMap).toWritableMap(sourceMap);
    }

    private static Exp getMapVal(Map<String, Object> qualifierMap, String[] dotPathArr,
                                 BinaryOperator<Exp> operator, Exp.Type expType, boolean useCtx) {
        Object obj = getValue1(qualifierMap).getObject();
        Exp mapExp;
        if (useCtx) {
            mapExp = MapExp.getByKey(MapReturnType.VALUE, expType,
                Exp.val(getValue2(qualifierMap).toString()), // VALUE2 contains key (field name)
                Exp.mapBin(getField(qualifierMap)), dotPathToCtxMapKeys(dotPathArr));
        } else {
            mapExp = MapExp.getByKey(MapReturnType.VALUE, expType,
                Exp.val(getValue2(qualifierMap).toString()),
                Exp.mapBin(getField(qualifierMap)));
        }

        return operator.apply(mapExp,
            toExp(getConverter(qualifierMap).toWritableValue(obj, TypeInformation.of(obj.getClass())))
        );
    }

    private static Exp getFilterExp(MappingAerospikeConverter converter, Value val, String field,
                                    BinaryOperator<Exp> function) {
        Object convertedValue = converter.toWritableValue(
            val.getObject(), TypeInformation.of(val.getObject().getClass())
        );
        if (convertedValue instanceof List<?>) {
            // Collection comes as JBLOB
            return function.apply(Exp.listBin(field), Exp.val((List<?>) convertedValue));
        } else {
            // custom objects are converted into Maps
            return function.apply(Exp.mapBin(field), Exp.val((Map<?, ?>) convertedValue));
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

    protected static Boolean ignoreCase(Map<String, Object> qualifierMap) {
        return (Boolean) qualifierMap.getOrDefault(IGNORE_CASE, false);
    }

    protected static int regexFlags(Map<String, Object> qualifierMap) {
        return ignoreCase(qualifierMap) ? RegexFlag.ICASE : RegexFlag.NONE;
    }

    protected static Value getValue1(Map<String, Object> qualifierMap) {
        return Value.get(qualifierMap.get(VALUE1));
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
