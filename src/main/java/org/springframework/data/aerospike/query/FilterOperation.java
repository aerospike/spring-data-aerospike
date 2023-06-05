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
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.query.Qualifier.QualifierBuilder;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import static com.aerospike.client.command.ParticleType.INTEGER;
import static com.aerospike.client.command.ParticleType.JBLOB;
import static com.aerospike.client.command.ParticleType.LIST;
import static com.aerospike.client.command.ParticleType.MAP;
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
        public Exp filterExp(Map<String, Object> map) {
            Qualifier[] qs = (Qualifier[]) map.get(QUALIFIERS);
            Exp[] childrenExp = new Exp[qs.length];
            for (int i = 0; i < qs.length; i++) {
                childrenExp[i] = qs[i].toFilterExp();
            }
            return Exp.and(childrenExp);
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return null;
        }
    },
    OR {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            Qualifier[] qs = (Qualifier[]) map.get(QUALIFIERS);
            Exp[] childrenExp = new Exp[qs.length];
            for (int i = 0; i < qs.length; i++) {
                childrenExp[i] = qs[i].toFilterExp();
            }
            return Exp.or(childrenExp);
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return null;
        }
    },
    IN {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            // Convert IN to a collection of OR as Aerospike has no support for IN query
            Value val = getValue1(map);
            int valType = val.getType();
            if (valType != LIST)
                throw new IllegalArgumentException(
                    "FilterOperation.IN expects List argument with type: " + LIST + ", instead got: " +
                        valType);
            List<?> inList = (List<?>) val.getObject();
            Exp[] listElementsExp = new Exp[inList.size()];

            for (int i = 0; i < inList.size(); i++) {
                listElementsExp[i] = new Qualifier(new QualifierBuilder()
                    .setField(getField(map))
                    .setFilterOperation(FilterOperation.EQ)
                    .setValue1(Value.get(inList.get(i)))
                ).toFilterExp();
            }
            return Exp.or(listElementsExp);
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return null;
        }
    },
    EQ {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            Value val = getValue1(map);
            return switch (val.getType()) {
                case INTEGER -> Exp.eq(Exp.intBin(getField(map)), Exp.val(val.toLong()));
                case STRING -> {
                    if (ignoreCase(map)) {
                        String equalsRegexp = QualifierRegexpBuilder.getStringEquals(getValue1(map).toString());
                        yield Exp.regexCompare(equalsRegexp, RegexFlag.ICASE, Exp.stringBin(getField(map)));
                    } else {
                        yield Exp.eq(Exp.stringBin(getField(map)), Exp.val(val.toString()));
                    }
                }
                case JBLOB -> getFilterExp(getConverter(map), val, getField(map), Exp::eq);
                case MAP -> getFilterExp(getConverter(map), val, getField(map), Exp::eq, Exp::mapBin);
                case LIST -> getFilterExp(getConverter(map), val, getField(map), Exp::eq, Exp::listBin);
                default -> throw new IllegalArgumentException("EQ FilterExpression unsupported particle type: " +
                    val.getClass().getSimpleName());
            };
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() == INTEGER) {
                return Filter.equal(getField(map), getValue1(map).toLong());
            } else {
                // There is no case-insensitive string comparison filter.
                if (ignoreCase(map)) {
                    return null;
                }
                return Filter.equal(getField(map), getValue1(map).toString());
            }
        }
    },
    NOTEQ {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            Value val = getValue1(map);
            return switch (val.getType()) {
                case INTEGER -> Exp.ne(Exp.intBin(getField(map)), Exp.val(val.toLong()));
                case STRING -> {
                    if (ignoreCase(map)) {
                        String equalsRegexp = QualifierRegexpBuilder.getStringEquals(getValue1(map).toString());
                        yield Exp.not(Exp.regexCompare(equalsRegexp, RegexFlag.ICASE, Exp.stringBin(getField(map))));
                    } else {
                        yield Exp.ne(Exp.stringBin(getField(map)), Exp.val(val.toString()));
                    }
                }
                case JBLOB -> getFilterExp(getConverter(map), val, getField(map), Exp::ne);
                case MAP -> getFilterExp(getConverter(map), val, getField(map), Exp::ne, Exp::mapBin);
                case LIST -> getFilterExp(getConverter(map), val, getField(map), Exp::ne, Exp::listBin);
                default -> throw new IllegalArgumentException("NOTEQ FilterExpression unsupported particle type: " +
                    val.getClass().getSimpleName());
            };
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return null; // not supported in the secondary index filter
        }
    },
    GT {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            Value val = getValue1(map);
            return switch (val.getType()) {
                case INTEGER -> Exp.gt(Exp.intBin(getField(map)), Exp.val(getValue1(map).toLong()));
                case STRING -> Exp.gt(Exp.stringBin(getField(map)), Exp.val(getValue1(map).toString()));
                case JBLOB -> getFilterExp(getConverter(map), val, getField(map), Exp::gt);
                case MAP -> getFilterExp(getConverter(map), val, getField(map), Exp::gt, Exp::mapBin);
                case LIST -> getFilterExp(getConverter(map), val, getField(map), Exp::gt, Exp::listBin);
                default -> throw new IllegalArgumentException("GT FilterExpression unsupported particle type: " +
                    val.getClass().getSimpleName());
            };
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            // Long.MAX_VALUE shall not be given as + 1 will cause overflow
            if (getValue1(map).getType() != INTEGER || getValue1(map).toLong() == Long.MAX_VALUE) {
                return null;
            }

            return Filter.range(getField(map), getValue1(map).toLong() + 1, Long.MAX_VALUE);
        }
    },
    GTEQ {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            Value val = getValue1(map);
            return switch (val.getType()) {
                case INTEGER -> Exp.ge(Exp.intBin(getField(map)), Exp.val(getValue1(map).toLong()));
                case STRING -> Exp.ge(Exp.stringBin(getField(map)), Exp.val(getValue1(map).toString()));
                case JBLOB -> getFilterExp(getConverter(map), val, getField(map), Exp::ge);
                case MAP -> getFilterExp(getConverter(map), val, getField(map), Exp::ge, Exp::mapBin);
                case LIST -> getFilterExp(getConverter(map), val, getField(map), Exp::ge, Exp::listBin);
                default -> throw new IllegalArgumentException("GTEQ FilterExpression unsupported particle type: " +
                    val.getClass().getSimpleName());
            };
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != INTEGER) {
                return null;
            }
            return Filter.range(getField(map), getValue1(map).toLong(), Long.MAX_VALUE);
        }
    },
    LT {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            Value val = getValue1(map);
            return switch (val.getType()) {
                case INTEGER -> Exp.lt(Exp.intBin(getField(map)), Exp.val(getValue1(map).toLong()));
                case STRING -> Exp.lt(Exp.stringBin(getField(map)), Exp.val(getValue1(map).toString()));
                case JBLOB -> getFilterExp(getConverter(map), val, getField(map), Exp::lt);
                case MAP -> getFilterExp(getConverter(map), val, getField(map), Exp::lt, Exp::mapBin);
                case LIST -> getFilterExp(getConverter(map), val, getField(map), Exp::lt, Exp::listBin);
                default -> throw new IllegalArgumentException("LT FilterExpression unsupported particle type: " +
                    val.getClass().getSimpleName());
            };
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            // Long.MIN_VALUE shall not be given as - 1 will cause overflow
            if (getValue1(map).getType() != INTEGER || getValue1(map).toLong() == Long.MIN_VALUE) {
                return null;
            }
            return Filter.range(getField(map), Long.MIN_VALUE, getValue1(map).toLong() - 1);
        }
    },
    LTEQ {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            Value val = getValue1(map);
            return switch (val.getType()) {
                case INTEGER -> Exp.le(Exp.intBin(getField(map)), Exp.val(getValue1(map).toLong()));
                case STRING -> Exp.le(Exp.stringBin(getField(map)), Exp.val(getValue1(map).toString()));
                case JBLOB -> getFilterExp(getConverter(map), val, getField(map), Exp::le);
                case MAP -> getFilterExp(getConverter(map), val, getField(map), Exp::le, Exp::mapBin);
                case LIST -> getFilterExp(getConverter(map), val, getField(map), Exp::le, Exp::listBin);
                default -> throw new IllegalArgumentException("LTEQ FilterExpression unsupported particle type: " +
                    val.getClass().getSimpleName());
            };
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != INTEGER) {
                return null;
            }
            return Filter.range(getField(map), Long.MIN_VALUE, getValue1(map).toLong());
        }
    },
    BETWEEN {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            validateEquality(getValue1(map).getType(), getValue2(map).getType(), map, "BETWEEN");

            return switch (getValue1(map).getType()) {
                case INTEGER -> Exp.and(
                    Exp.ge(Exp.intBin(getField(map)), Exp.val(getValue1(map).toLong())),
                    Exp.lt(Exp.intBin(getField(map)), Exp.val(getValue2(map).toLong()))
                );
                case STRING -> Exp.and(
                    Exp.ge(Exp.stringBin(getField(map)), Exp.val(getValue1(map).toString())),
                    Exp.lt(Exp.stringBin(getField(map)), Exp.val(getValue2(map).toString()))
                );
                case JBLOB -> Exp.and(
                    getFilterExp(getConverter(map), getValue1(map), getField(map), Exp::ge),
                    getFilterExp(getConverter(map), getValue2(map), getField(map), Exp::lt)
                );
                case MAP -> Exp.and(
                    getFilterExp(getConverter(map), getValue1(map), getField(map), Exp::ge, Exp::mapBin),
                    getFilterExp(getConverter(map), getValue2(map), getField(map), Exp::lt, Exp::mapBin)
                );
                case LIST -> Exp.and(
                    getFilterExp(getConverter(map), getValue1(map), getField(map), Exp::ge, Exp::listBin),
                    getFilterExp(getConverter(map), getValue2(map), getField(map), Exp::lt, Exp::listBin)
                );
                default ->
                    throw new IllegalArgumentException("BETWEEN: unexpected value of type " + getValue1(map).getClass()
                        .getSimpleName());
            };
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != INTEGER || getValue2(map).getType() != INTEGER) {
                return null;
            }
            return Filter.range(getField(map), getValue1(map).toLong(), getValue2(map).toLong());
        }
    },
    STARTS_WITH {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            String startWithRegexp = QualifierRegexpBuilder.getStartsWith(getValue1(map).toString());
            return Exp.regexCompare(startWithRegexp, regexFlags(map), Exp.stringBin(getField(map)));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return null; // String secondary index does not support "starts with" queries
        }
    },
    ENDS_WITH {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            String endWithRegexp = QualifierRegexpBuilder.getEndsWith(getValue1(map).toString());
            return Exp.regexCompare(endWithRegexp, regexFlags(map), Exp.stringBin(getField(map)));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return null; // String secondary index does not support "ends with" queries
        }
    },
    CONTAINING {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            String containingRegexp = QualifierRegexpBuilder.getContaining(getValue1(map).toString());
            return Exp.regexCompare(containingRegexp, regexFlags(map), Exp.stringBin(getField(map)));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return null; // String secondary index does not support "contains" queries
        }
    },
    LIKE {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            int flags = RegexFlag.EXTENDED;
            if (ignoreCase(map)) {
                flags = RegexFlag.EXTENDED | RegexFlag.ICASE;
            }
            return Exp.regexCompare(getValue1(map).toString(), flags, Exp.stringBin(getField(map)));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return null; // not supported
        }
    },
    MAP_VAL_EQ_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            return getFilterExpMapValEqOrFail(map, Exp::eq, "MAP_VAL_EQ_BY_KEY");
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            String[] dotPathArr = getDotPathArray(getDotPath(map),
                "MAP_VAL_EQ_BY_KEY secondary index filter: dotPath has not been set");
            final boolean useCtx = dotPathArr.length > 2;

            return switch (getValue1(map).getType()) {
                case STRING -> {
                    if (ignoreCase(map)) { // there is no case-insensitive string comparison filter
                        yield null; // MAP_VALUE_EQ_BY_KEY sIndexFilter: case-insensitive comparison is not supported
                    }
                    if (useCtx) {
                        yield null; // currently not supported
                    } else {
                        yield Filter.contains(getField(map), IndexCollectionType.MAPVALUES,
                            getValue1(map).toString());
                    }
                }
                case INTEGER -> {
                    if (useCtx) {
                        yield null; // currently not supported
                    } else {
                        yield Filter.range(getField(map), IndexCollectionType.MAPVALUES, getValue1(map).toLong(),
                            getValue1(map).toLong());
                    }
                }
                default -> null;
            };
        }
    },
    MAP_VAL_NOTEQ_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            return getFilterExpMapValEqOrFail(map, Exp::ne, "MAP_VAL_NOTEQ_BY_KEY");
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return null; // not supported
        }
    },
    MAP_VAL_GT_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            return getFilterExpMapValOrFail(map, Exp::gt, "MAP_VAL_GT_BY_KEY");
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            // Long.MAX_VALUE shall not be given as + 1 will cause overflow
            if (getValue1(map).getType() != INTEGER || getValue1(map).toLong() == Long.MAX_VALUE) {
                return null;
            }

            String[] dotPathArr = getDotPathArray(getDotPath(map),
                "MAP_VAL_GT_BY_KEY secondary index filter: dotPath has not been set");
            if (dotPathArr.length > 2) {
                return null; // currently not supported
            } else {
                return Filter.range(getField(map), IndexCollectionType.MAPVALUES, getValue1(map).toLong() + 1,
                    Long.MAX_VALUE);
            }
        }
    },
    MAP_VAL_GTEQ_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            return getFilterExpMapValOrFail(map, Exp::ge, "MAP_VAL_GTEQ_BY_KEY");
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != INTEGER) {
                return null;
            }

            String[] dotPathArr = getDotPathArray(getDotPath(map),
                "MAP_VAL_GTEQ_BY_KEY secondary index filter: dotPath has not been set");
            if (dotPathArr.length > 2) {
                return null; // currently not supported
            } else {
                return Filter.range(getField(map), IndexCollectionType.MAPVALUES, getValue1(map).toLong(),
                    Long.MAX_VALUE);
            }
        }
    },
    MAP_VAL_LT_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            return getFilterExpMapValOrFail(map, Exp::lt, "MAP_VAL_LT_BY_KEY");
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            // Long.MIN_VALUE shall not be given as - 1 will cause overflow
            if (getValue1(map).getType() != INTEGER || getValue1(map).toLong() == Long.MIN_VALUE) {
                return null;
            }

            String[] dotPathArr = getDotPathArray(getDotPath(map),
                "MAP_VAL_LT_BY_KEY secondary index filter: dotPath has not been set");
            if (dotPathArr.length > 2) {
                return null; // currently not supported
            } else {
                return Filter.range(getField(map), IndexCollectionType.MAPVALUES, Long.MIN_VALUE,
                    getValue1(map).toLong() - 1);
            }
        }
    },
    MAP_VAL_LTEQ_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            return getFilterExpMapValOrFail(map, Exp::le, "MAP_VAL_LTEQ_BY_KEY");
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != INTEGER) {
                return null;
            }

            String[] dotPathArr = getDotPathArray(getDotPath(map),
                "MAP_VAL_LTEQ_BY_KEY secondary index filter: dotPath has not been set");
            if (dotPathArr.length > 2) {
                return null; // currently not supported
            } else {
                return Filter.range(getField(map), IndexCollectionType.MAPVALUES, Long.MIN_VALUE,
                    getValue1(map).toLong());
            }
        }
    },
    MAP_VAL_BETWEEN_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            String[] dotPathArr = getDotPathArray(getDotPath(map),
                "MAP_VAL_BETWEEN_BY_KEY filter expression: dotPath has not been set");

            // VALUE2 contains key (field name), VALUE3 contains upper limit
            validateEquality(getValue1(map).getType(), getValue3(map).getType(), map, "MAP_VAL_BETWEEN_BY_KEY");

            Exp value1, value2;
            Exp.Type type;
            switch (getValue1(map).getType()) {
                case INTEGER -> {
                    value1 = Exp.val(getValue1(map).toLong());
                    value2 = Exp.val(getValue3(map).toLong());
                    type = Exp.Type.INT;
                }
                case STRING -> {
                    value1 = Exp.val(getValue1(map).toString());
                    value2 = Exp.val(getValue3(map).toString());
                    type = Exp.Type.STRING;
                }
                case JBLOB -> {
                    Object convertedValue1 = getConvertedValue(map, FilterOperation::getValue1);
                    Object convertedValue3 = getConvertedValue(map, FilterOperation::getValue3);
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
                    value1 = Exp.val((List<?>) getValue1(map).getObject());
                    value2 = Exp.val((List<?>) getValue3(map).getObject());
                    type = Exp.Type.LIST;
                }
                case MAP -> {
                    value1 = Exp.val((Map<?, ?>) getValue1(map).getObject());
                    value2 = Exp.val((Map<?, ?>) getValue3(map).getObject());
                    type = Exp.Type.MAP;
                }
                default -> throw new IllegalArgumentException(
                    "MAP_VAL_BETWEEN_BY_KEY FilterExpression unsupported type: expected integer, long or String, " +
                        "instead got " + getValue1(map).getClass().getSimpleName());
            }

            return mapValBetweenByKey(map, dotPathArr, type, value1, value2);
        }

        private static Exp mapValBetweenByKey(Map<String, Object> map, String[] dotPathArr, Exp.Type type, Exp lowerLimit,
                                       Exp upperLimit) {
            Exp mapExp;
            if (dotPathArr.length > 2) {
                mapExp = MapExp.getByKey(MapReturnType.VALUE, type, Exp.val(getValue2(map).toString()),
                    Exp.mapBin(getField(map)), dotPathToCtxMapKeys(dotPathArr));
            } else {
                mapExp = MapExp.getByKey(MapReturnType.VALUE, type, Exp.val(getValue2(map).toString()),
                    Exp.mapBin(getField(map)));
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
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != INTEGER || getValue3(map).getType() != INTEGER) {
                return null;
            }

            String[] dotPathArr = getDotPathArray(getDotPath(map),
                "MAP_VAL_BETWEEN_BY_KEY secondary index filter: dotPath has not been set");
            if (dotPathArr.length > 2) {
                return null; // currently not supported
            } else {
                return Filter.range(getField(map), IndexCollectionType.MAPVALUES, getValue1(map).toLong(),
                    getValue3(map).toLong());
            }
        }
    },
    MAP_VAL_STARTS_WITH_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            String startWithRegexp = QualifierRegexpBuilder.getStartsWith(getValue1(map).toString());

            return Exp.regexCompare(startWithRegexp, regexFlags(map),
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getValue2(map).toString()),
                    Exp.mapBin(getField(map)))
            );
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return null; // String secondary index does not support "starts with" queries
        }
    },
    MAP_VAL_LIKE_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            int flags = RegexFlag.EXTENDED;
            if (ignoreCase(map)) {
                flags = RegexFlag.EXTENDED | RegexFlag.ICASE;
            }
            return Exp.regexCompare(getValue1(map).toString(), flags,
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getValue2(map).toString()),
                    Exp.mapBin(getField(map)))
            );
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return null; // not supported
        }
    },
    MAP_VAL_ENDS_WITH_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            String endWithRegexp = QualifierRegexpBuilder.getEndsWith(getValue1(map).toString());

            return Exp.regexCompare(endWithRegexp, regexFlags(map),
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getValue2(map).toString()),
                    Exp.mapBin(getField(map)))
            );
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return null; // String secondary index does not support "ends with" queries
        }
    },
    MAP_VAL_CONTAINING_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            String containingRegexp = QualifierRegexpBuilder.getContaining(getValue1(map).toString());
            return Exp.regexCompare(containingRegexp, regexFlags(map),
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getValue2(map).toString()),
                    Exp.mapBin(getField(map)))
            );
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return null; // String secondary index does not support "contains" queries
        }
    },
    MAP_KEYS_CONTAIN {
        @Override
        public Exp filterExp(Map<String, Object> map) {

            Exp value;
            switch (getValue1(map).getType()) {
                case INTEGER -> {
                    value = Exp.val(getValue1(map).toLong());
                }
                case STRING -> {
                    value = Exp.val(getValue1(map).toString());
                }
                case JBLOB -> {
                    Object convertedValue = getConvertedValue(map, FilterOperation::getValue1);
                    if (convertedValue instanceof List<?>) {
                        // Collection comes as JBLOB
                        value = Exp.val((List<?>) convertedValue);
                    } else {
                        // custom objects are converted into Maps
                        value = Exp.val((Map<?, ?>) convertedValue);
                    }
                }
                case LIST -> {
                    value = Exp.val((List<?>) getValue1(map).getObject());
                }
                case MAP -> {
                    value = Exp.val((Map<?, ?>) getValue1(map).getObject());
                }
                default -> throw new IllegalArgumentException(
                    "MAP_KEYS_CONTAIN FilterExpression unsupported type: expected integer, long or String, " +
                        "instead got " + getValue1(map).getClass().getSimpleName());
            }

            return Exp.gt(
                MapExp.getByKey(MapReturnType.COUNT, Exp.Type.INT, value, Exp.mapBin(getField(map))),
                Exp.val(0));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return collectionContains(IndexCollectionType.MAPKEYS, map);
        }
    },
    MAP_VALUES_CONTAIN {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            Exp value;
            switch (getValue1(map).getType()) {
                case INTEGER -> {
                    value = Exp.val(getValue1(map).toLong());
                }
                case STRING -> {
                    value = Exp.val(getValue1(map).toString());
                }
                case JBLOB -> {
                    Object convertedValue = getConvertedValue(map, FilterOperation::getValue1);
                    if (convertedValue instanceof List<?>) {
                        // Collection comes as JBLOB
                        value = Exp.val((List<?>) convertedValue);
                    } else {
                        // custom objects are converted into Maps
                        value = Exp.val((Map<?, ?>) convertedValue);
                    }
                }
                case LIST -> {
                    value = Exp.val((List<?>) getValue1(map).getObject());
                }
                case MAP -> {
                    value = Exp.val((Map<?, ?>) getValue1(map).getObject());
                }
                default -> throw new IllegalArgumentException(
                    "MAP_VALUES_CONTAIN FilterExpression unsupported type: expected integer, long or String, " +
                        "instead got " + getValue1(map).getClass().getSimpleName());
            }

            return Exp.gt(
                MapExp.getByValue(MapReturnType.COUNT, value, Exp.mapBin(getField(map))),
                Exp.val(0));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return collectionContains(IndexCollectionType.MAPVALUES, map);
        }
    },
    MAP_KEYS_BETWEEN {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            validateEquality(getValue1(map).getType(), getValue2(map).getType(), map, "MAP_KEYS_BETWEEN");

            Exp value1, value2;
            switch (getValue1(map).getType()) {
                case INTEGER -> {
                    value1 = Exp.val(getValue1(map).toLong());
                    value2 = Exp.val(getValue2(map).toLong());
                }
                case STRING -> {
                    value1 = Exp.val(getValue1(map).toString());
                    value2 = Exp.val(getValue2(map).toString());
                }
                case JBLOB -> {
                    Object convertedValue1 = getConvertedValue(map, FilterOperation::getValue1);
                    Object convertedValue2 = getConvertedValue(map, FilterOperation::getValue2);
                    if (convertedValue1 instanceof List<?>) {
                        // Collection comes as JBLOB
                        value1 = Exp.val((List<?>) convertedValue1);
                        value2 = Exp.val((List<?>) convertedValue2);
                    } else {
                        // custom objects are converted into Maps
                        value1 = Exp.val((Map<?, ?>) convertedValue1);
                        value2 = Exp.val((Map<?, ?>) convertedValue2);
                    }
                }
                case LIST -> {
                    value1 = Exp.val((List<?>) getValue1(map).getObject());
                    value2 = Exp.val((List<?>) getValue2(map).getObject());
                }
                case MAP -> {
                    value1 = Exp.val((Map<?, ?>) getValue1(map).getObject());
                    value2 = Exp.val((Map<?, ?>) getValue2(map).getObject());
                }
                default -> throw new IllegalArgumentException(
                    "MAP_KEYS_BETWEEN FilterExpression unsupported type: expected integer, long or String, instead got "
                        + getValue1(map).getClass().getSimpleName());
            }

            return Exp.gt(
                MapExp.getByKeyRange(MapReturnType.COUNT, value1, value2, Exp.mapBin(getField(map))),
                Exp.val(0));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return collectionRange(IndexCollectionType.MAPKEYS, map);
        }
    },
    MAP_VAL_BETWEEN {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            validateEquality(getValue1(map).getType(), getValue2(map).getType(), map, "MAP_VAL_BETWEEN");

            Exp value1, value2;
            switch (getValue1(map).getType()) {
                case INTEGER -> {
                    value1 = Exp.val(getValue1(map).toLong());
                    value2 = Exp.val(getValue2(map).toLong());
                }
                case STRING -> {
                    value1 = Exp.val(getValue1(map).toString());
                    value2 = Exp.val(getValue2(map).toString());
                }
                case JBLOB -> {
                    Object convertedValue1 = getConvertedValue(map, FilterOperation::getValue1);
                    Object convertedValue2 = getConvertedValue(map, FilterOperation::getValue2);
                    if (convertedValue1 instanceof List<?>) {
                        // Collection comes as JBLOB
                        value1 = Exp.val((List<?>) convertedValue1);
                        value2 = Exp.val((List<?>) convertedValue2);
                    } else {
                        // custom objects are converted into Maps
                        value1 = Exp.val((Map<?, ?>) convertedValue1);
                        value2 = Exp.val((Map<?, ?>) convertedValue2);
                    }
                }
                case LIST -> {
                    value1 = Exp.val((List<?>) getValue1(map).getObject());
                    value2 = Exp.val((List<?>) getValue2(map).getObject());
                }
                case MAP -> {
                    value1 = Exp.val((Map<?, ?>) getValue1(map).getObject());
                    value2 = Exp.val((Map<?, ?>) getValue2(map).getObject());
                }
                default -> throw new IllegalArgumentException(
                    "MAP_VAL_BETWEEN FilterExpression unsupported type: expected integer, long or String, instead got "
                        + getValue1(map).getClass().getSimpleName());
            }

            return Exp.gt(
                MapExp.getByValueRange(MapReturnType.COUNT, value1, value2, Exp.mapBin(getField(map))),
                Exp.val(0));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != INTEGER || getValue2(map).getType() != INTEGER) {
                return null;
            }
            return collectionRange(IndexCollectionType.MAPVALUES, map);
        }
    },
    GEO_WITHIN {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            return Exp.geoCompare(Exp.geoBin(getField(map)), Exp.geo(getValue1(map).toString()));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return geoWithinRadius(IndexCollectionType.DEFAULT, map);
        }
    },
    LIST_VAL_CONTAINING {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            Exp value;
            switch (getValue1(map).getType()) {
                case INTEGER -> {
                    value = Exp.val(getValue1(map).toLong());
                }
                case STRING -> {
                    value = Exp.val(getValue1(map).toString());
                }
                case JBLOB -> {
                    Object convertedValue = getConvertedValue(map, FilterOperation::getValue1);
                    if (convertedValue instanceof List<?>) {
                        // Collection comes as JBLOB
                        value = Exp.val((List<?>) convertedValue);
                    } else {
                        // custom objects are converted into Maps
                        value = Exp.val((Map<?, ?>) convertedValue);
                    }
                }
                case LIST -> {
                    value = Exp.val((List<?>) getValue1(map).getObject());
                }
                case MAP -> {
                    value = Exp.val((Map<?, ?>) getValue1(map).getObject());
                }
                default -> throw new IllegalArgumentException(
                    "LIST_VAL_CONTAINING FilterExpression unsupported type: expected integer, long or String, " +
                        "instead got " + getValue1(map).getClass().getSimpleName());
            }

            return Exp.gt(
                ListExp.getByValue(ListReturnType.COUNT, value, Exp.listBin(getField(map))),
                Exp.val(0));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != STRING && getValue1(map).getType() != INTEGER) {
                return null;
            }

            return collectionContains(IndexCollectionType.LIST, map);
        }
    },
    LIST_VAL_BETWEEN {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            validateEquality(getValue1(map).getType(), getValue2(map).getType(), map, "LIST_VAL_BETWEEN");

            Exp value1, value2;
            switch (getValue1(map).getType()) {
                case INTEGER -> {
                    value1 = Exp.val(getValue1(map).toLong());
                    value2 = Exp.val(getValue2(map).toLong());
                }
                case STRING -> {
                    value1 = Exp.val(getValue1(map).toString());
                    value2 = Exp.val(getValue2(map).toString());
                }
                case JBLOB -> {
                    Object convertedValue1 = getConvertedValue(map, FilterOperation::getValue1);
                    Object convertedValue2 = getConvertedValue(map, FilterOperation::getValue2);
                    if (convertedValue1 instanceof List<?>) {
                        // Collection comes as JBLOB
                        value1 = Exp.val((List<?>) convertedValue1);
                        value2 = Exp.val((List<?>) convertedValue2);
                    } else {
                        // custom objects are converted into Maps
                        value1 = Exp.val((Map<?, ?>) convertedValue1);
                        value2 = Exp.val((Map<?, ?>) convertedValue2);
                    }
                }
                case LIST -> {
                    value1 = Exp.val((List<?>) getValue1(map).getObject());
                    value2 = Exp.val((List<?>) getValue2(map).getObject());
                }
                case MAP -> {
                    value1 = Exp.val((Map<?, ?>) getValue1(map).getObject());
                    value2 = Exp.val((Map<?, ?>) getValue2(map).getObject());
                }
                default -> throw new IllegalArgumentException(
                    "LIST_VAL_BETWEEN FilterExpression unsupported type: expected integer, long or String, instead got "
                        + getValue1(map).getClass().getSimpleName());
            }

            return Exp.gt(
                ListExp.getByValueRange(ListReturnType.COUNT, value1, value2, Exp.listBin(getField(map))),
                Exp.val(0));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != INTEGER || getValue2(map).getType() != INTEGER) {
                return null;
            }

            return collectionRange(IndexCollectionType.LIST, map); // both limits are inclusive
        }
    },
    LIST_VAL_GT {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            if (getValue1(map).getType() == INTEGER) {
                if (getValue1(map).toLong() == Long.MAX_VALUE) {
                    throw new IllegalArgumentException(
                        "LIST_VAL_GT FilterExpression unsupported value: expected [Long.MIN_VALUE.." +
                            "Long.MAX_VALUE-1]");
                }

                return Exp.gt(
                    ListExp.getByValueRange(ListReturnType.COUNT, Exp.val(getValue1(map).toLong() + 1L),
                        null, Exp.listBin(getField(map))),
                    Exp.val(0)
                );
            } else {
                Exp value = switch (getValue1(map).getType()) {
                    case STRING -> Exp.val(getValue1(map).toString());
                    case JBLOB -> {
                        Object convertedValue = getConvertedValue(map, FilterOperation::getValue1);
                        if (convertedValue instanceof List<?>) {
                            // Collection comes as JBLOB
                            yield Exp.val((List<?>) convertedValue);
                        } else {
                            // custom objects are converted into Maps
                            yield Exp.val((Map<?, ?>) convertedValue);
                        }
                    }
                    case LIST -> Exp.val((List<?>) getValue1(map).getObject());
                    case MAP -> Exp.val((Map<?, ?>) getValue1(map).getObject());
                    default -> throw new IllegalArgumentException(
                        "LIST_VAL_GT FilterExpression unsupported type: expected integer, long or String, instead got "
                            + getValue1(map).getClass().getSimpleName());
                };

                Exp rangeIncludingValue = ListExp.getByValueRange(ListReturnType.COUNT, value, null,
                    Exp.listBin(getField(map)));
                Exp valueOnly = ListExp.getByValue(ListReturnType.COUNT, value, Exp.listBin(getField(map)));
                return Exp.gt(Exp.sub(rangeIncludingValue, valueOnly), Exp.val(0));
            }
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            // Long.MAX_VALUE shall not be given as + 1 will cause overflow
            if (getValue1(map).getType() != INTEGER || getValue1(map).toLong() == Long.MAX_VALUE) {
                return null;
            }

            return Filter.range(getField(map), IndexCollectionType.LIST, getValue1(map).toLong() + 1, Long.MAX_VALUE);
        }
    },
    LIST_VAL_GTEQ {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            Exp value = switch (getValue1(map).getType()) {
                case INTEGER -> Exp.val(getValue1(map).toLong());
                case STRING -> Exp.val(getValue1(map).toString());
                case JBLOB -> {
                    Object convertedValue = getConvertedValue(map, FilterOperation::getValue1);
                    if (convertedValue instanceof List<?>) {
                        // Collection comes as JBLOB
                        yield Exp.val((List<?>) convertedValue);
                    } else {
                        // custom objects are converted into Maps
                        yield Exp.val((Map<?, ?>) convertedValue);
                    }
                }
                case LIST -> Exp.val((List<?>) getValue1(map).getObject());
                case MAP -> Exp.val((Map<?, ?>) getValue1(map).getObject());
                default -> throw new IllegalArgumentException(
                    "LIST_VAL_GTEQ FilterExpression unsupported type: expected integer, long or String, instead got "
                        + getValue1(map).getClass().getSimpleName());
            };

            return Exp.gt(
                ListExp.getByValueRange(ListReturnType.COUNT, value, null, Exp.listBin(getField(map))),
                Exp.val(0));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != INTEGER) {
                return null;
            }

            return Filter.range(getField(map), IndexCollectionType.LIST, getValue1(map).toLong(), Long.MAX_VALUE);
        }
    },
    LIST_VAL_LT {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            Exp value = switch (getValue1(map).getType()) {
                case INTEGER -> {
                    if (getValue1(map).toLong() == Long.MIN_VALUE) {
                        throw new IllegalArgumentException(
                            "LIST_VAL_LT FilterExpression unsupported value: expected [Long.MIN_VALUE+1.." +
                                "Long.MAX_VALUE]");
                    }

                    yield Exp.val(getValue1(map).toLong());
                }
                case STRING -> Exp.val(getValue1(map).toString());
                case JBLOB -> {
                    Object convertedValue = getConvertedValue(map, FilterOperation::getValue1);
                    if (convertedValue instanceof List<?>) {
                        // Collection comes as JBLOB
                        yield Exp.val((List<?>) convertedValue);
                    } else {
                        // custom objects are converted into Maps
                        yield Exp.val((Map<?, ?>) convertedValue);
                    }
                }
                case LIST -> Exp.val((List<?>) getValue1(map).getObject());
                case MAP -> Exp.val((Map<?, ?>) getValue1(map).getObject());
                default -> throw new IllegalArgumentException(
                    "LIST_VAL_GTEQ FilterExpression unsupported type: expected integer, long or String, instead got "
                        + getValue1(map).getClass().getSimpleName());
            };

            return Exp.gt(
                ListExp.getByValueRange(ListReturnType.COUNT, null, value, Exp.listBin(getField(map))),
                Exp.val(0));
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            // Long.MIN_VALUE shall not be given as - 1 will cause overflow
            if (getValue1(map).getType() != INTEGER || getValue1(map).toLong() == Long.MIN_VALUE) {
                return null;
            }

            return Filter.range(getField(map), IndexCollectionType.LIST, Long.MIN_VALUE, getValue1(map).toLong() - 1);
        }
    },
    LIST_VAL_LTEQ {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            if (getValue1(map).getType() == INTEGER) {
                Exp upperLimit;
                if (getValue1(map).toLong() == Long.MAX_VALUE) {
                    upperLimit = Exp.inf();
                } else {
                    upperLimit = Exp.val(getValue1(map).toLong() + 1L);
                }

                return Exp.gt(
                    ListExp.getByValueRange(ListReturnType.COUNT, Exp.val(Long.MIN_VALUE),
                        upperLimit, Exp.listBin(getField(map))),
                    Exp.val(0));
            } else {
                Exp value = switch (getValue1(map).getType()) {
                    case STRING -> Exp.val(getValue1(map).toString());
                    case JBLOB -> {
                        Object convertedValue = getConvertedValue(map, FilterOperation::getValue1);
                        if (convertedValue instanceof List<?>) {
                            // Collection comes as JBLOB
                            yield Exp.val((List<?>) convertedValue);
                        } else {
                            // custom objects are converted into Maps
                            yield Exp.val((Map<?, ?>) convertedValue);
                        }
                    }
                    case LIST -> Exp.val((List<?>) getValue1(map).getObject());
                    case MAP -> Exp.val((Map<?, ?>) getValue1(map).getObject());
                    default -> throw new IllegalArgumentException(
                        "LIST_VAL_LTEQ FilterExpression unsupported type: expected integer, long or String, instead " +
                            "got " + getValue1(map).getClass().getSimpleName());
                };

                Exp rangeIncludingValue = ListExp.getByValueRange(ListReturnType.COUNT, null, value,
                    Exp.listBin(getField(map)));
                Exp valueOnly = ListExp.getByValue(ListReturnType.COUNT, value, Exp.listBin(getField(map)));
                return Exp.gt(Exp.add(rangeIncludingValue, valueOnly), Exp.val(0));
            }
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != INTEGER) {
                return null;
            }

            return Filter.range(getField(map), IndexCollectionType.LIST, Long.MIN_VALUE, getValue1(map).toLong());
        }
    };

    private static void validateEquality(int type1, int type2, Map<String, Object> map, String opName) {
        if (type1 != type2) {
            throw new IllegalArgumentException(opName + ": expected both parameters to have the same "
                + "type, instead got " + getValue1(map).getClass().getSimpleName() + " and " +
                getValue2(map).getClass().getSimpleName());
        }
    }

    /**
     * FilterOperations that require both sIndexFilter and FilterExpression
     */
    public static final List<FilterOperation> dualFilterOperations = Arrays.asList(
        MAP_VAL_EQ_BY_KEY, MAP_VAL_GT_BY_KEY, MAP_VAL_GTEQ_BY_KEY, MAP_VAL_LT_BY_KEY, MAP_VAL_LTEQ_BY_KEY,
        MAP_VAL_BETWEEN_BY_KEY
    );

    private static Exp getFilterExpMapValOrFail(Map<String, Object> map, BinaryOperator<Exp> operator, String opName) {
        String[] dotPathArr = getDotPathArray(getDotPath(map),
            opName + " filter expression: dotPath has not been set");
        Exp mapExp;

        // VALUE2 contains key (field name)
        // currently only String Map keys are supported
        if (dotPathArr.length > 2) {
            mapExp = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2(map).toString()),
                Exp.mapBin(getField(map)), dotPathToCtxMapKeys(dotPathArr));
        } else {
            mapExp = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2(map).toString()),
                Exp.mapBin(getField(map)));
        }

        return switch (getValue1(map).getType()) {
            case INTEGER -> operator.apply(mapExp, Exp.val(getValue1(map).toLong()));
            case STRING -> operator.apply(mapExp, Exp.val(getValue1(map).toString()));
            case JBLOB -> {
                Object convertedValue = getConvertedValue(map, FilterOperation::getValue1);
                if (convertedValue instanceof List<?>) {
                    // Collection comes as JBLOB
                    yield operator.apply(mapExp, Exp.val((List<?>) convertedValue));
                } else {
                    // custom objects are converted into Maps
                    yield operator.apply(mapExp, Exp.val((Map<?, ?>) convertedValue));
                }
            }
            case LIST -> operator.apply(mapExp, Exp.val((List<?>) getValue1(map).getObject()));
            case MAP -> operator.apply(mapExp, Exp.val((Map<?, ?>) getValue1(map).getObject()));
            default -> throw new IllegalArgumentException(
                opName + " FilterExpression unsupported type: " + getValue1(map).getClass().getSimpleName()
                    + ", expected integer, long or String");
        };
    }

    private static Exp getFilterExpMapValEqOrFail(Map<String, Object> map, BinaryOperator<Exp> operator,
                                                  String opName) {
        String[] dotPathArr = getDotPathArray(getDotPath(map),
            opName + " filter expression: dotPath has not been set");
        final boolean useCtx = dotPathArr.length > 2;
        Exp mapExp;

        return switch (getValue1(map).getType()) {
            case INTEGER -> {
                if (useCtx) {
                    mapExp = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                        Exp.val(getValue2(map).toString()), // VALUE2 contains key (field name)
                        Exp.mapBin(getField(map)), dotPathToCtxMapKeys(dotPathArr));
                } else {
                    mapExp = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                        Exp.val(getValue2(map).toString()),
                        Exp.mapBin(getField(map)));
                }

                yield operator.apply(mapExp, Exp.val(getValue1(map).toLong()));
            }
            case STRING -> {
                if (ignoreCase(map)) {
                    throw new IllegalArgumentException(
                        opName + " FilterExpression: case insensitive comparison is not supported");
                }
                if (useCtx) {
                    mapExp = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                        Exp.val(getValue2(map).toString()), // VALUE2 contains key (field name)
                        Exp.mapBin(getField(map)), dotPathToCtxMapKeys(dotPathArr));
                } else {
                    mapExp = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                        Exp.val(getValue2(map).toString()),
                        Exp.mapBin(getField(map)));
                }

                yield operator.apply(mapExp, Exp.val(getValue1(map).toString()));
            }
            case JBLOB -> {
                if (getConvertedValue(map, FilterOperation::getValue1) instanceof List<?>) {
                    // Collection comes as JBLOB
                    yield getMapVal(map, dotPathArr, operator, Exp.Type.LIST, useCtx);
                } else {
                    // custom objects are converted into Maps
                    yield getMapVal(map, dotPathArr, operator, Exp.Type.MAP, useCtx);
                }
            }
            case LIST -> getMapVal(map, dotPathArr, operator, Exp.Type.LIST, useCtx);
            case MAP -> getMapVal(map, dotPathArr, operator, Exp.Type.MAP, useCtx);
            default -> throw new IllegalArgumentException(
                opName + " FilterExpression unsupported type: " + getValue1(map).getClass().getSimpleName()
                    + ", expected integer, long, String, Map or Object");
        };
    }

    private static Object getConvertedValue(Map<String, Object> map, Function<Map<String, Object>, Value> function) {
        return getConverter(map).toWritableValue(
            function.apply(map).getObject(), TypeInformation.of(function.apply(map).getObject().getClass())
        );
    }

    private static Exp getMapVal(Map<String, Object> map, String[] dotPathArr,
                                 BinaryOperator<Exp> operator, Exp.Type expType, boolean useCtx) {
        Object obj = getValue1(map).getObject();
        Exp mapExp;
        if (useCtx) {
            mapExp = MapExp.getByKey(MapReturnType.VALUE, expType,
                Exp.val(getValue2(map).toString()), // VALUE2 contains key (field name)
                Exp.mapBin(getField(map)), dotPathToCtxMapKeys(dotPathArr));
        } else {
            mapExp = MapExp.getByKey(MapReturnType.VALUE, expType,
                Exp.val(getValue2(map).toString()),
                Exp.mapBin(getField(map)));
        }

        return operator.apply(mapExp,
            toExp(getConverter(map).toWritableValue(obj, TypeInformation.of(obj.getClass())))
        );
    }

    private static Exp getFilterExp(MappingAerospikeConverter converter, Value val, String field,
                                    BinaryOperator<Exp> function) {
        Object convertedValue = converter.toWritableValue(
            val.getObject(), TypeInformation.of(val.getObject().getClass())
        );
        if (convertedValue instanceof List<?>) {
            // Collection comes as JBLOB
            return function.apply(Exp.listBin(field), toExp(convertedValue));
        } else {
            // custom objects are converted into Maps
            return function.apply(Exp.mapBin(field), toExp(convertedValue));
        }
    }

    private static Exp getFilterExp(MappingAerospikeConverter converter, Value val, String field,
                                    BinaryOperator<Exp> operator, Function<String, Exp> binExp) {
        Object convertedValue = converter.toWritableValue(
            val.getObject(), TypeInformation.of(val.getObject().getClass())
        );
        return operator.apply(binExp.apply(field), toExp(convertedValue));
    }

    private static String[] getDotPathArray(String dotPath, String errMsg) {
        if (StringUtils.hasLength(dotPath)) {
            return dotPath.split("\\.");
        } else {
            throw new IllegalArgumentException(errMsg);
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
        } else {
            throw new IllegalArgumentException("Unsupported type for converting: " + value.getClass()
                .getCanonicalName());
        }

        return res;
    }

    protected static String getField(Map<String, Object> map) {
        return (String) map.get(FIELD);
    }

    protected static Boolean ignoreCase(Map<String, Object> map) {
        return (Boolean) map.getOrDefault(IGNORE_CASE, false);
    }

    protected static int regexFlags(Map<String, Object> map) {
        return ignoreCase(map) ? RegexFlag.ICASE : RegexFlag.NONE;
    }

    protected static Value getValue1(Map<String, Object> map) {
        return Value.get(map.get(VALUE1));
    }

    protected static Value getValue2(Map<String, Object> map) {
        return Value.get(map.get(VALUE2));
    }

    protected static Value getValue3(Map<String, Object> map) {
        return (Value) map.get(VALUE3);
    }

    protected static String getDotPath(Map<String, Object> map) {
        return (String) map.get(DOT_PATH);
    }

    protected static MappingAerospikeConverter getConverter(Map<String, Object> map) {
        return (MappingAerospikeConverter) map.get(CONVERTER);
    }

    public abstract Exp filterExp(Map<String, Object> map);

    public abstract Filter sIndexFilter(Map<String, Object> map);

    protected Filter collectionContains(IndexCollectionType collectionType, Map<String, Object> map) {
        Value val = getValue1(map);
        int valType = val.getType();
        return switch (valType) {
            case INTEGER -> Filter.contains(getField(map), collectionType, val.toLong());
            case STRING -> Filter.contains(getField(map), collectionType, val.toString());
            default -> null;
        };
    }

    protected Filter collectionRange(IndexCollectionType collectionType, Map<String, Object> map) {
        return Filter.range(getField(map), collectionType, getValue1(map).toLong(), getValue2(map).toLong());
    }

    @SuppressWarnings("SameParameterValue")
    protected Filter geoWithinRadius(IndexCollectionType collectionType, Map<String, Object> map) {
        return Filter.geoContains(getField(map), getValue1(map).toString());
    }
}
