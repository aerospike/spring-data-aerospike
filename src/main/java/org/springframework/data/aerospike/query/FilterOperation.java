package org.springframework.data.aerospike.query;

import com.aerospike.client.AerospikeException;
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
import org.springframework.data.util.TypeInformation;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

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
            if (valType != ParticleType.LIST)
                throw new IllegalArgumentException(
                    "FilterOperation.IN expects List argument with type: " + ParticleType.LIST + ", but got: " +
                        valType);
            List<?> inList = (List<?>) val.getObject();
            Exp[] listElementsExp = new Exp[inList.size()];

            for (int i = 0; i < inList.size(); i++) {
                listElementsExp[i] = new Qualifier(new Qualifier.QualifierBuilder()
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
                case ParticleType.INTEGER -> Exp.eq(Exp.intBin(getField(map)), Exp.val(val.toLong()));
                case ParticleType.STRING -> {
                    if (ignoreCase(map)) {
                        String equalsRegexp = QualifierRegexpBuilder.getStringEquals(getValue1(map).toString());
                        yield Exp.regexCompare(equalsRegexp, RegexFlag.ICASE, Exp.stringBin(getField(map)));
                    } else {
                        yield Exp.eq(Exp.stringBin(getField(map)), Exp.val(val.toString()));
                    }
                }
                case ParticleType.JBLOB, ParticleType.MAP -> Exp.eq(
                    Exp.mapBin(getField(map)), toExp(getConverter(map).toWritableValue(
                            val.getObject(), TypeInformation.of(val.getObject().getClass())
                        )
                    )
                );
                default ->
                    throw new AerospikeException("EQ FilterExpression unsupported particle type: " + val.getType());
            };
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() == ParticleType.INTEGER) {
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
                case ParticleType.INTEGER -> Exp.ne(Exp.intBin(getField(map)), Exp.val(val.toLong()));
                case ParticleType.STRING -> {
                    if (ignoreCase(map)) {
                        String equalsRegexp = QualifierRegexpBuilder.getStringEquals(getValue1(map).toString());
                        yield Exp.not(Exp.regexCompare(equalsRegexp, RegexFlag.ICASE, Exp.stringBin(getField(map))));
                    } else {
                        yield Exp.ne(Exp.stringBin(getField(map)), Exp.val(val.toString()));
                    }
                }
                case ParticleType.JBLOB, ParticleType.MAP -> Exp.ne(
                    Exp.mapBin(getField(map)), toExp(getConverter(map).toWritableValue(
                            val.getObject(), TypeInformation.of(val.getObject().getClass())
                        )
                    )
                );
                default ->
                    throw new AerospikeException("NOTEQ FilterExpression unsupported particle type: " + val.getType());
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
                case ParticleType.INTEGER -> Exp.gt(Exp.intBin(getField(map)), Exp.val(getValue1(map).toLong()));
                case ParticleType.STRING -> Exp.gt(Exp.stringBin(getField(map)), Exp.val(getValue1(map).toString()));
                case ParticleType.JBLOB, ParticleType.MAP -> Exp.gt(
                    Exp.mapBin(getField(map)), toExp(getConverter(map).toWritableValue(
                            val.getObject(), TypeInformation.of(val.getObject().getClass())
                        )
                    )
                );
                default ->
                    throw new AerospikeException("GT FilterExpression unsupported particle type: " + val.getType());
            };
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            // Long.MAX_VALUE shall not be given as + 1 will cause overflow
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue1(map).toLong() == Long.MAX_VALUE) {
                throw new AerospikeException(
                    "GT sIndexFilter unsupported type: expected [Long.MIN_VALUE..Long.MAX_VALUE-1]");
            }

            return Filter.range(getField(map), getValue1(map).toLong() + 1, Long.MAX_VALUE);
        }
    },
    GTEQ {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            Value val = getValue1(map);
            return switch (val.getType()) {
                case ParticleType.INTEGER -> Exp.ge(Exp.intBin(getField(map)), Exp.val(getValue1(map).toLong()));
                case ParticleType.STRING -> Exp.ge(Exp.stringBin(getField(map)), Exp.val(getValue1(map).toString()));
                case ParticleType.JBLOB, ParticleType.MAP -> Exp.ge(
                    Exp.mapBin(getField(map)), toExp(getConverter(map).toWritableValue(
                            val.getObject(), TypeInformation.of(val.getObject().getClass())
                        )
                    )
                );
                default ->
                    throw new AerospikeException("GTEQ FilterExpression unsupported particle type: " + val.getType());
            };
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException("GTEQ sIndexFilter unsupported type: expected Long");
            }
            return Filter.range(getField(map), getValue1(map).toLong(), Long.MAX_VALUE);
        }
    },
    LT {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            Value val = getValue1(map);
            return switch (val.getType()) {
                case ParticleType.INTEGER -> Exp.lt(Exp.intBin(getField(map)), Exp.val(getValue1(map).toLong()));
                case ParticleType.STRING -> Exp.lt(Exp.stringBin(getField(map)), Exp.val(getValue1(map).toString()));
                case ParticleType.JBLOB, ParticleType.MAP -> Exp.lt(
                    Exp.mapBin(getField(map)), toExp(getConverter(map).toWritableValue(
                            val.getObject(), TypeInformation.of(val.getObject().getClass())
                        )
                    )
                );
                default ->
                    throw new AerospikeException("LT FilterExpression unsupported particle type: " + val.getType());
            };
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            // Long.MIN_VALUE shall not be given as - 1 will cause overflow
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue1(map).toLong() == Long.MIN_VALUE) {
                throw new AerospikeException(
                    "LT sIndexFilter unsupported type: expected [Long.MIN_VALUE+1..Long.MAX_VALUE]");
            }
            return Filter.range(getField(map), Long.MIN_VALUE, getValue1(map).toLong() - 1);
        }
    },
    LTEQ {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            Value val = getValue1(map);
            return switch (val.getType()) {
                case ParticleType.INTEGER -> Exp.le(Exp.intBin(getField(map)), Exp.val(getValue1(map).toLong()));
                case ParticleType.STRING -> Exp.le(Exp.stringBin(getField(map)), Exp.val(getValue1(map).toString()));
                case ParticleType.JBLOB, ParticleType.MAP -> Exp.le(
                    Exp.mapBin(getField(map)), toExp(getConverter(map).toWritableValue(
                            val.getObject(), TypeInformation.of(val.getObject().getClass())
                        )
                    )
                );
                default ->
                    throw new AerospikeException("LTEQ FilterExpression unsupported particle type: " + val.getType());
            };
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException("LTEQ sIndexFilter unsupported type: expected Long");
            }
            return Filter.range(getField(map), Long.MIN_VALUE, getValue1(map).toLong());
        }
    },
    BETWEEN {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            if (getValue1(map).getType() == ParticleType.INTEGER && getValue2(map).getType() == ParticleType.INTEGER) {
                return Exp.and(
                    Exp.ge(Exp.intBin(getField(map)), Exp.val(getValue1(map).toLong())),
                    Exp.le(Exp.intBin(getField(map)), Exp.val(getValue2(map).toLong()))
                );
            }
            throw new AerospikeException("BETWEEN FilterExpression unsupported type: expected Long");
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue2(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException("BETWEEN sIndexFilter unsupported type: expected Long");
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
            return null; // String secondary index does not support "contains" queries
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
            return null; // String secondary index does not support "contains" queries
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
    MAP_VALUE_EQ_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            String[] dotPathArr = getDotPathArray(getDotPath(map),
                "MAP_VALUE_EQ_BY_KEY filter expression: dotPath has not been set");
            final boolean useCtx = dotPathArr.length > 2;
            Exp mapExp;

            return switch (getValue1(map).getType()) {
                case ParticleType.STRING -> {
                    if (ignoreCase(map)) {
                        throw new IllegalArgumentException(
                            "MAP_VALUE_EQ_BY_KEY FilterExpression: case insensitive comparison is not supported");
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

                    yield Exp.eq(mapExp, Exp.val(getValue1(map).toString()));
                }
                case ParticleType.INTEGER -> {
                    if (useCtx) {
                        mapExp = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(getValue2(map).toString()), // VALUE2 contains key (field name)
                            Exp.mapBin(getField(map)), dotPathToCtxMapKeys(dotPathArr));
                    } else {
                        mapExp = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(getValue2(map).toString()),
                            Exp.mapBin(getField(map)));
                    }

                    yield Exp.eq(mapExp, Exp.val(getValue1(map).toLong()));
                }
                case ParticleType.JBLOB -> {
                    Object obj = getValue1(map).getObject();
                    if (useCtx) {
                        mapExp = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.MAP,
                            Exp.val(getValue2(map).toString()), // VALUE2 contains key (field name)
                            Exp.mapBin(getField(map)), dotPathToCtxMapKeys(dotPathArr));
                    } else {
                        mapExp = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.MAP,
                            Exp.val(getValue2(map).toString()),
                            Exp.mapBin(getField(map)));
                    }

                    yield Exp.eq(mapExp,
                        toExp(getConverter(map).toWritableValue(obj, TypeInformation.of(obj.getClass())))
                    );
                }
                default -> throw new AerospikeException(
                    "MAP_VALUE_EQ_BY_KEY FilterExpression unsupported type: " + getValue1(map).getType());
            };
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            String[] dotPathArr = getDotPathArray(getDotPath(map),
                "MAP_VALUE_EQ_BY_KEY secondary index filter: dotPath has not been set");
            final boolean useCtx = dotPathArr.length > 2;

            return switch (getValue1(map).getType()) {
                case ParticleType.STRING -> {
                    if (ignoreCase(map)) { // there is no case-insensitive string comparison filter
                        throw new IllegalArgumentException(
                            "MAP_VALUE_EQ_BY_KEY sIndexFilter: case insensitive comparison is not supported");
                    }
                    if (useCtx) {
                        yield Filter.contains(getField(map), IndexCollectionType.MAPKEYS,
                            dotPathArr[dotPathArr.length - 1], dotPathToCtxMapKeys(dotPathArr));
                    } else {
                        yield Filter.contains(getField(map), IndexCollectionType.MAPVALUES,
                            getValue1(map).toString());
                    }
                }
                case ParticleType.INTEGER -> {
                    if (useCtx) {
                        yield Filter.contains(getField(map), IndexCollectionType.MAPKEYS,
                            dotPathArr[dotPathArr.length - 1], dotPathToCtxMapKeys(dotPathArr));
                    } else {
                        yield Filter.range(getField(map), IndexCollectionType.MAPVALUES, getValue1(map).toLong(),
                            getValue1(map).toLong());
                    }
                }
                default -> throw new AerospikeException(
                    "MAP_VALUE_EQ_BY_KEY FilterExpression unsupported type: " + getValue1(map).getType());
            };
        }
    },
    MAP_VALUE_NOTEQ_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            String[] dotPathArr = getDotPathArray(getDotPath(map),
                "MAP_VALUE_NOTEQ_BY_KEY filter expression: dotPath has not been set");
            final boolean useCtx = dotPathArr.length > 2;
            Exp mapExp;

            return switch (getValue1(map).getType()) {
                case ParticleType.STRING -> {
                    if (ignoreCase(map)) {
                        throw new IllegalArgumentException(
                            "MAP_VALUE_NOTEQ_BY_KEY FilterExpression: case insensitive comparison is not supported");
                    }
                    if (useCtx) {
                        mapExp = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                            Exp.val(getValue2(map).toString()),
                            Exp.mapBin(getField(map)), dotPathToCtxMapKeys(dotPathArr));
                    } else {
                        mapExp = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                            Exp.val(getValue2(map).toString()),
                            Exp.mapBin(getField(map)));
                    }

                    yield Exp.ne(mapExp, Exp.val(getValue1(map).toString()));
                }
                case ParticleType.INTEGER -> {
                    if (useCtx) {
                        mapExp = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(getValue2(map).toString()), // VALUE2 contains key (field name)
                            Exp.mapBin(getField(map)), dotPathToCtxMapKeys(dotPathArr));
                    } else {
                        mapExp = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(getValue2(map).toString()),
                            Exp.mapBin(getField(map)));
                    }

                    yield Exp.ne(mapExp, Exp.val(getValue1(map).toLong()));
                }
                case ParticleType.JBLOB -> {
                    Object obj = getValue1(map).getObject();
                    if (useCtx) {
                        mapExp = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.MAP,
                            Exp.val(getValue2(map).toString()), // VALUE2 contains key (field name)
                            Exp.mapBin(getField(map)), dotPathToCtxMapKeys(dotPathArr));
                    } else {
                        mapExp = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.MAP,
                            Exp.val(getValue2(map).toString()),
                            Exp.mapBin(getField(map)));
                    }

                    yield Exp.ne(mapExp,
                        toExp(getConverter(map).toWritableValue(obj, TypeInformation.of(obj.getClass())))
                    );
                }
                default -> throw new AerospikeException(
                    "MAP_VALUE_NOTEQ_BY_KEY FilterExpression unsupported type: " + getValue1(map).getType());
            };
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return null; // not supported
        }
    },
    MAP_VALUE_GT_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            // VALUE2 contains key (field name)
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Exp.gt(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2(map).toString()),
                        Exp.mapBin(getField(map))),
                    Exp.val(getValue1(map).toLong())
                );
            }
            throw new AerospikeException(
                "MAP_VALUE_GT_BY_KEY FilterExpression unsupported type: expected Long");
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            // Long.MAX_VALUE shall not be given as + 1 will cause overflow
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue1(map).toLong() == Long.MAX_VALUE) {
                throw new AerospikeException(
                    "MAP_VALUE_GT_BY_KEY sIndexFilter unsupported type: expected [Long.MIN_VALUE..Long.MAX_VALUE-1]");
            }

            return Filter.range(getField(map), IndexCollectionType.MAPVALUES, getValue1(map).toLong() + 1,
                Long.MAX_VALUE);
        }
    },
    MAP_VALUE_GTEQ_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            // VALUE2 contains key (field name)
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Exp.ge(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2(map).toString()),
                        Exp.mapBin(getField(map))),
                    Exp.val(getValue1(map).toLong())
                );
            }
            throw new AerospikeException(
                "MAP_VALUE_GTEQ_BY_KEY FilterExpression unsupported type: expected Long");
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException(
                    "MAP_VALUE_GTEQ_BY_KEY sIndexFilter unsupported type: expected Long");
            }

            return Filter.range(getField(map), IndexCollectionType.MAPVALUES, getValue1(map).toLong(), Long.MAX_VALUE);
        }
    },
    MAP_VALUE_LT_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            // VALUE2 contains key (field name)
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Exp.lt(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2(map).toString()),
                        Exp.mapBin(getField(map))),
                    Exp.val(getValue1(map).toLong())
                );
            }
            throw new AerospikeException(
                "MAP_VALUE_LT_BY_KEY FilterExpression unsupported type: expected Long");
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            // Long.MIN_VALUE shall not be given as - 1 will cause overflow
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue1(map).toLong() == Long.MIN_VALUE) {
                throw new AerospikeException(
                    "MAP_VALUE_LT_BY_KEY sIndexFilter unsupported type: expected [Long.MIN_VALUE+1..Long.MAX_VALUE]");
            }

            return Filter.range(getField(map), IndexCollectionType.MAPVALUES, Long.MIN_VALUE,
                getValue1(map).toLong() - 1);
        }
    },
    MAP_VALUE_LTEQ_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            // VALUE2 contains key (field name)
            if (getValue1(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException(
                    "MAP_VALUE_LTEQ_BY_KEY FilterExpression unsupported type: expected Long");
            }

            return Exp.le(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2(map).toString()),
                    Exp.mapBin(getField(map))),
                Exp.val(getValue1(map).toLong())
            );
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException(
                    "MAP_VALUE_LTEQ_BY_KEY sIndexFilter unsupported type: expected Long");
            }

            return Filter.range(getField(map), IndexCollectionType.MAPVALUES, Long.MIN_VALUE, getValue1(map).toLong());
        }
    },
    MAP_VALUES_BETWEEN_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            // VALUE2 contains key (field name), VALUE3 contains upper limit
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue3(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException(
                    "MAP_VALUES_BETWEEN_BY_KEY FilterExpression unsupported type: expected Long");
            }

            return Exp.and(
                Exp.ge(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2(map).toString()),
                        Exp.mapBin(getField(map))),
                    Exp.val(getValue1(map).toLong())
                ),
                Exp.le(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2(map).toString()),
                        Exp.mapBin(getField(map))),
                    Exp.val(getValue3(map).toLong())
                )
            );
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue3(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException(
                    "MAP_VALUES_BETWEEN_BY_KEY sIndex filter unsupported type: expected Long");
            }

            return Filter.range(getField(map), IndexCollectionType.MAPVALUES, getValue1(map).toLong(),
                getValue3(map).toLong());
        }
    },
    MAP_VALUE_STARTS_WITH_BY_KEY {
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
            return null; // String secondary index does not support "contains" queries
        }
    },
    MAP_VALUE_ENDS_WITH_BY_KEY {
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
            return null; // String secondary index does not support "contains" queries
        }
    },
    MAP_VALUE_CONTAINING_BY_KEY {
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
            return switch (getValue1(map).getType()) {
                case ParticleType.STRING -> Exp.gt(
                    MapExp.getByKey(MapReturnType.COUNT, Exp.Type.INT, Exp.val(getValue1(map).toString()),
                        Exp.mapBin(getField(map))),
                    Exp.val(0)
                );
                case ParticleType.INTEGER -> Exp.gt(
                    MapExp.getByKey(MapReturnType.COUNT, Exp.Type.INT, Exp.val(getValue1(map).toLong()),
                        Exp.mapBin(getField(map))),
                    Exp.val(0)
                );
                default -> throw new AerospikeException(
                    "MAP_KEYS_CONTAINS FilterExpression unsupported type: expected String or Long");
            };
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return collectionContains(IndexCollectionType.MAPKEYS, map);
        }
    },
    MAP_VALUES_CONTAIN {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            return switch (getValue1(map).getType()) {
                case ParticleType.STRING -> Exp.gt(
                    MapExp.getByValue(MapReturnType.COUNT, Exp.val(getValue1(map).toString()),
                        Exp.mapBin(getField(map))),
                    Exp.val(0)
                );
                case ParticleType.INTEGER -> Exp.gt(
                    MapExp.getByValue(MapReturnType.COUNT, Exp.val(getValue1(map).toLong()),
                        Exp.mapBin(getField(map))),
                    Exp.val(0)
                );
                default -> throw new AerospikeException(
                    "MAP_VALUES_CONTAINS FilterExpression unsupported type: expected String or Long");
            };
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return collectionContains(IndexCollectionType.MAPVALUES, map);
        }
    },
    MAP_KEYS_BETWEEN {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue2(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException(
                    "MAP_KEYS_BETWEEN FilterExpression unsupported type: expected Long");
            }

            // + 1L to the valueEnd since the valueEnd is exclusive
            Exp upperLimit = Exp.val(getValue2(map).toLong() + 1L);

            // Long.MAX_VALUE will not be processed correctly if given as an inclusive parameter as it will cause
            // overflow
            if (getValue2(map).toLong() == Long.MAX_VALUE) upperLimit = null;


            return Exp.gt(
                // + 1L to the valueEnd since the valueEnd is exclusive
                MapExp.getByKeyRange(MapReturnType.COUNT, Exp.val(getValue1(map).toLong()), upperLimit,
                    Exp.mapBin(getField(map))),
                Exp.val(0)
            );
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return collectionRange(IndexCollectionType.MAPKEYS, map);
        }
    },
    MAP_VALUES_BETWEEN {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue2(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException(
                    "MAP_VALUES_BETWEEN FilterExpression unsupported type: expected Long");
            }

            // + 1L to the valueEnd since the valueEnd is exclusive (both begin and values should be included).
            Exp upperLimit = Exp.val(getValue2(map).toLong() + 1L);

            // Long.MAX_VALUE will not be processed correctly if given as an inclusive parameter as it will cause
            // overflow
            if (getValue2(map).toLong() == Long.MAX_VALUE) upperLimit = null;


            return Exp.gt(
                // + 1L to the valueEnd since the valueEnd is exclusive
                MapExp.getByValueRange(MapReturnType.COUNT, Exp.val(getValue1(map).toLong()), upperLimit,
                    Exp.mapBin(getField(map))),
                Exp.val(0)
            );
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue2(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException(
                    "MAP_VALUES_BETWEEN sIndex filter unsupported type: expected Long");
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
    LIST_CONTAINS {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            return switch (getValue1(map).getType()) {
                case ParticleType.STRING -> Exp.gt(
                    ListExp.getByValue(ListReturnType.COUNT, Exp.val(getValue1(map).toString()),
                        Exp.listBin(getField(map))),
                    Exp.val(0)
                );
                case ParticleType.INTEGER -> Exp.gt(
                    ListExp.getByValue(ListReturnType.COUNT, Exp.val(getValue1(map).toLong()),
                        Exp.listBin(getField(map))),
                    Exp.val(0)
                );
                default -> throw new AerospikeException(
                    "LIST_CONTAINS FilterExpression unsupported type: expected String or Long");
            };
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.STRING && getValue1(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException(
                    "LIST_CONTAINS sIndexFilter unsupported type: expected String or Long");
            }

            return collectionContains(IndexCollectionType.LIST, map);
        }
    },
    LIST_VALUE_BETWEEN {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue2(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException(
                    "LIST_BETWEEN FilterExpression unsupported type: expected Long");
            }

            // + 1L to the valueEnd since the valueEnd is exclusive
            Exp upperLimit = Exp.val(getValue2(map).toLong() + 1L);

            // Long.MAX_VALUE will not be processed correctly if given an inclusive parameter as it will cause overflow
            if (getValue2(map).toLong() == Long.MAX_VALUE) upperLimit = null;

            return Exp.gt(
                ListExp.getByValueRange(ListReturnType.COUNT, Exp.val(getValue1(map).toLong()), upperLimit,
                    Exp.listBin(getField(map))),
                Exp.val(0)
            );
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue2(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException(
                    "LIST_BETWEEN sIndexFilter unsupported type: expected Long");
            }

            return collectionRange(IndexCollectionType.LIST, map);
        }
    },
    LIST_VALUE_GT {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue1(map).toLong() == Long.MAX_VALUE) {
                throw new AerospikeException(
                    "LIST_VALUE_GT FilterExpression unsupported type: expected [Long.MIN_VALUE..Long.MAX_VALUE-1]");
            }

            return Exp.gt(
                ListExp.getByValueRange(ListReturnType.COUNT, Exp.val(getValue1(map).toLong() + 1L),
                    Exp.val(Long.MAX_VALUE), Exp.listBin(getField(map))),
                Exp.val(0)
            );
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            // Long.MAX_VALUE shall not be given as + 1 will cause overflow
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue1(map).toLong() == Long.MAX_VALUE) {
                throw new AerospikeException(
                    "LIST_VALUE_GT sIndexFilter unsupported type: expected [Long.MIN_VALUE..Long.MAX_VALUE-1]");
            }

            return Filter.range(getField(map), IndexCollectionType.LIST, getValue1(map).toLong() + 1, Long.MAX_VALUE);
        }
    },
    LIST_VALUE_GTEQ {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException(
                    "LIST_VALUE_GTEQ FilterExpression unsupported type: expected Long");
            }

            return Exp.gt(
                ListExp.getByValueRange(ListReturnType.COUNT, Exp.val(getValue1(map).toLong()), null,
                    Exp.listBin(getField(map))),
                Exp.val(0)
            );
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException(
                    "LIST_VALUE_GTEQ sIndexFilter unsupported type: expected Long");
            }

            return Filter.range(getField(map), IndexCollectionType.LIST, getValue1(map).toLong(), Long.MAX_VALUE);
        }
    },
    LIST_VALUE_LT {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue1(map).toLong() == Long.MIN_VALUE) {
                throw new AerospikeException(
                    "LIST_VALUE_LT FilterExpression unsupported type: expected [Long.MIN_VALUE+1..Long.MAX_VALUE]");
            }

            return Exp.gt(
                ListExp.getByValueRange(ListReturnType.COUNT, Exp.val(Long.MIN_VALUE),
                    Exp.val(getValue1(map).toLong() - 1L), Exp.listBin(getField(map))),
                Exp.val(0)
            );
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            // Long.MIN_VALUE shall not be given as - 1 will cause overflow
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue1(map).toLong() == Long.MIN_VALUE) {
                throw new AerospikeException(
                    "LIST_VALUE_LT sIndexFilter unsupported type: expected [Long.MIN_VALUE+1..Long.MAX_VALUE]");
            }

            return Filter.range(getField(map), IndexCollectionType.LIST, Long.MIN_VALUE, getValue1(map).toLong() - 1);
        }
    },
    LIST_VALUE_LTEQ {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException(
                    "LIST_VALUE_LTEQ FilterExpression unsupported type: expected Long");
            }

            // + 1L to the valueEnd since the valueEnd is exclusive
            Exp upperLimit = Exp.val(getValue1(map).toLong() + 1L);

            // Long.MIN_VALUE will not be processed correctly as - 1 will cause overflow
            if (getValue1(map).toLong() == Long.MAX_VALUE) upperLimit = null;

            return Exp.gt(
                ListExp.getByValueRange(ListReturnType.COUNT, Exp.val(Long.MIN_VALUE), upperLimit,
                    Exp.listBin(getField(map))),
                Exp.val(0)
            );
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException(
                    "LIST_VALUE_LTEQ sIndexFilter unsupported type: expected Long");
            }

            return Filter.range(getField(map), IndexCollectionType.LIST, Long.MIN_VALUE, getValue1(map).toLong());
        }
    };

    private static String[] getDotPathArray(String dotPath, String errMsg) {
        if (StringUtils.hasLength(dotPath)) {
            return dotPath.split("\\.");
        } else {
            throw new IllegalArgumentException(errMsg);
        }
    }

    /**
     * FilterOperations that require both sIndexFilter and FilterExpression
     */
    public static final List<FilterOperation> dualFilterOperations = Arrays.asList(
        MAP_VALUE_EQ_BY_KEY, MAP_VALUE_GT_BY_KEY, MAP_VALUE_GTEQ_BY_KEY, MAP_VALUE_LT_BY_KEY, MAP_VALUE_LTEQ_BY_KEY,
        MAP_VALUES_BETWEEN_BY_KEY
    );

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

    public abstract Exp filterExp(Map<String, Object> map);

    public abstract Filter sIndexFilter(Map<String, Object> map);

    protected String getField(Map<String, Object> map) {
        return (String) map.get(FIELD);
    }

    protected Boolean ignoreCase(Map<String, Object> map) {
        return (Boolean) map.getOrDefault(IGNORE_CASE, false);
    }

    protected int regexFlags(Map<String, Object> map) {
        return ignoreCase(map) ? RegexFlag.ICASE : RegexFlag.NONE;
    }

    protected Value getValue1(Map<String, Object> map) {
        return (Value) map.get(VALUE1);
    }

    protected Value getValue2(Map<String, Object> map) {
        return (Value) map.get(VALUE2);
    }

    protected Value getValue3(Map<String, Object> map) {
        return (Value) map.get(VALUE3);
    }

    protected String getDotPath(Map<String, Object> map) {
        return (String) map.get(DOT_PATH);
    }

    protected MappingAerospikeConverter getConverter(Map<String, Object> map) {
        return (MappingAerospikeConverter) map.get(CONVERTER);
    }

    protected Filter collectionContains(IndexCollectionType collectionType, Map<String, Object> map) {
        Value val = getValue1(map);
        int valType = val.getType();
        return switch (valType) {
            case ParticleType.INTEGER -> Filter.contains(getField(map), collectionType, val.toLong());
            case ParticleType.STRING -> Filter.contains(getField(map), collectionType, val.toString());
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
