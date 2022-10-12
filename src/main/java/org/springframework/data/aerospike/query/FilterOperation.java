package org.springframework.data.aerospike.query;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.query.RegexFlag;

import java.util.List;
import java.util.Map;

import static org.springframework.data.aerospike.query.Qualifier.*;

public enum FilterOperation {
    AND {
        @Override
        public Exp process(Map<String, Object> map) {
            Qualifier[] qs = (Qualifier[]) map.get(QUALIFIERS);
            Exp[] childrenExp = new Exp[qs.length];
            for (int i = 0; i < qs.length; i++) {
                childrenExp[i] = qs[i].toFilterExp();
            }
            return Exp.and(childrenExp);
        }
    },
    OR {
        @Override
        public Exp process(Map<String, Object> map) {
            Qualifier[] qs = (Qualifier[]) map.get(QUALIFIERS);
            Exp[] childrenExp = new Exp[qs.length];
            for (int i = 0; i < qs.length; i++) {
                childrenExp[i] = qs[i].toFilterExp();
            }
            return Exp.or(childrenExp);
        }
    },
    IN {
        @Override
        public Exp process(Map<String, Object> map) { // Convert IN to a collection of or as Aerospike has not support for IN query
            Value val = getValue1(map);
            int valType = val.getType();
            if (valType != ParticleType.LIST)
                throw new IllegalArgumentException("FilterOperation.IN expects List argument with type: " + ParticleType.LIST + ", but got: " + valType);
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
    },
    EQ {
        @Override
        public Exp process(Map<String, Object> map) {
            Exp exp;
            Value val = getValue1(map);
            int valType = val.getType();
            switch (valType) {
                case ParticleType.INTEGER:
                    exp = Exp.eq(Exp.intBin(getField(map)), Exp.val(val.toLong()));
                    break;
                case ParticleType.STRING:
                    if (ignoreCase(map)) {
                        String equalsRegexp = QualifierRegexpBuilder.getStringEquals(getValue1(map).toString());
                        exp = Exp.regexCompare(equalsRegexp, RegexFlag.ICASE, Exp.stringBin(getField(map)));
                    } else {
                        exp = Exp.eq(Exp.stringBin(getField(map)), Exp.val(val.toString()));
                    }
                    break;
                default:
                    throw new AerospikeException("FilterExpression unsupported particle type: " + valType);
            }

            return exp;
        }
    },
    NOTEQ {
        @Override
        public Exp process(Map<String, Object> map) {
            Value val = getValue1(map);
            int valType = val.getType();
            Exp exp;

            if (valType == ParticleType.INTEGER) {
                exp = Exp.ne(Exp.intBin(getField(map)), Exp.val(val.toLong()));
            } else {
                exp = Exp.ne(Exp.stringBin(getField(map)), Exp.val(val.toString()));
            }
            return exp;
        }
    },
    GT {
        @Override
        public Exp process(Map<String, Object> map) {
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Exp.gt(Exp.intBin(getField(map)), Exp.val(getValue1(map).toLong()));
            }
            throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation GT)");
        }
    },
    GTEQ {
        @Override
        public Exp process(Map<String, Object> map) {
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Exp.ge(Exp.intBin(getField(map)), Exp.val(getValue1(map).toLong()));
            }
            throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation GTEQ)");
        }
    },
    LT {
        @Override
        public Exp process(Map<String, Object> map) {
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Exp.lt(Exp.intBin(getField(map)), Exp.val(getValue1(map).toLong()));
            }
            throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation LT)");
        }
    },
    LTEQ {
        @Override
        public Exp process(Map<String, Object> map) {
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Exp.le(Exp.intBin(getField(map)), Exp.val(getValue1(map).toLong()));
            }
            throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation LTEQ)");
        }
    },
    BETWEEN {
        @Override
        public Exp process(Map<String, Object> map) {
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Exp.and(
                        Exp.ge(Exp.intBin(getField(map)), Exp.val(getValue1(map).toLong())),
                        Exp.le(Exp.intBin(getField(map)), Exp.val(getValue2(map).toLong()))
                );
            }
            throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation BETWEEN)");
        }
    },
    STARTS_WITH {
        @Override
        public Exp process(Map<String, Object> map) {
            String startWithRegexp = QualifierRegexpBuilder.getStartsWith(getValue1(map).toString());
            return Exp.regexCompare(startWithRegexp, regexFlags(map), Exp.stringBin(getField(map)));
        }
    },
    ENDS_WITH {
        @Override
        public Exp process(Map<String, Object> map) {
            String endWithRegexp = QualifierRegexpBuilder.getEndsWith(getValue1(map).toString());
            return Exp.regexCompare(endWithRegexp, regexFlags(map), Exp.stringBin(getField(map)));
        }
    },
    CONTAINING {
        @Override
        public Exp process(Map<String, Object> map) {
            String containingRegexp = QualifierRegexpBuilder.getContaining(getValue1(map).toString());
            return Exp.regexCompare(containingRegexp, regexFlags(map), Exp.stringBin(getField(map)));
        }
    },
    MAP_VALUE_EQ_BY_KEY {
        @Override
        public Exp process(Map<String, Object> map) {
            // VALUE2 contains key (field name)
            Exp exp;
            switch (getValue1(map).getType()) {
                case ParticleType.STRING:
                    exp = Exp.eq(
                            MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getValue2(map).toString()), Exp.mapBin(getField(map))),
                            Exp.val(getValue1(map).toString()));
                    break;
                case ParticleType.INTEGER:
                    exp = Exp.eq(
                            MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2(map).toString()), Exp.mapBin(getField(map))),
                            Exp.val(getValue1(map).toLong()));
                    break;
                default:
                    throw new AerospikeException("FilterExpression unsupported type: expected String or Long (FilterOperation MAP_VALUE_EQ_BY_KEY)");
            };

            return exp;
        }
    },
    MAP_VALUE_NOTEQ_BY_KEY {
        @Override
        public Exp process(Map<String, Object> map) {
            Exp exp;
            switch (getValue1(map).getType()) {
                case ParticleType.STRING:
                    exp = Exp.ne(
                            MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getValue2(map).toString()), Exp.mapBin(getField(map))),
                            Exp.val(getValue1(map).toString()));
                    break;
                case ParticleType.INTEGER:
                    exp = Exp.ne(
                            MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2(map).toString()), Exp.mapBin(getField(map))),
                            Exp.val(getValue1(map).toLong()));
                    break;
                default:
                    throw new AerospikeException("FilterExpression unsupported type: expected String or Long (FilterOperation MAP_VALUE_NOTEQ_BY_KEY)");
            }

            return exp;
        }
    },
    MAP_VALUE_GT_BY_KEY {
        @Override
        public Exp process(Map<String, Object> map) {
            // VALUE2 contains key (field name)
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Exp.gt(
                        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2(map).toString()), Exp.mapBin(getField(map))),
                        Exp.val(getValue1(map).toLong())
                );
            }
            throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation MAP_VALUE_GT_BY_KEY)");
        }
    },
    MAP_VALUE_GTEQ_BY_KEY {
        @Override
        public Exp process(Map<String, Object> map) {
            // VALUE2 contains key (field name)
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Exp.ge(
                        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2(map).toString()), Exp.mapBin(getField(map))),
                        Exp.val(getValue1(map).toLong())
                );
            }
            throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation MAP_VALUE_GTEQ_BY_KEY)");
        }
    },
    MAP_VALUE_LT_BY_KEY {
        @Override
        public Exp process(Map<String, Object> map) {
            // VALUE2 contains key (field name)
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Exp.lt(
                        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2(map).toString()), Exp.mapBin(getField(map))),
                        Exp.val(getValue1(map).toLong())
                );
            }
            throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation MAP_VALUE_LT_BY_KEY)");
        }
    },
    MAP_VALUE_LTEQ_BY_KEY {
        @Override
        public Exp process(Map<String, Object> map) {
            // VALUE2 contains key (field name)
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Exp.le(
                        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2(map).toString()), Exp.mapBin(getField(map))),
                        Exp.val(getValue1(map).toLong())
                );
            }
            throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation MAP_VALUE_LTEQ_BY_KEY)");
        }
    },
    MAP_VALUES_BETWEEN_BY_KEY {
        @Override
        public Exp process(Map<String, Object> map) {
            // VALUE2 contains key (field name), VALUE3 contains upper limit
            if (getValue1(map).getType() == ParticleType.INTEGER && getValue3(map).getType() == ParticleType.INTEGER) {
                return Exp.and(
                        Exp.ge(
                                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2(map).toString()), Exp.mapBin(getField(map))),
                                Exp.val(getValue1(map).toLong())
                        ),
                        Exp.le(
                                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2(map).toString()), Exp.mapBin(getField(map))),
                                Exp.val(getValue3(map).toLong())
                        )
                );
            }
            throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation MAP_VALUE_BETWEEN_BY_KEY)");
        }
    },
    MAP_VALUE_STARTS_WITH_BY_KEY {
        @Override
        public Exp process(Map<String, Object> map) {
            String startWithRegexp = QualifierRegexpBuilder.getStartsWith(getValue1(map).toString());
            return Exp.regexCompare(startWithRegexp, regexFlags(map),
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getValue2(map).toString()), Exp.mapBin(getField(map)))
            );
        }
    },
    MAP_VALUE_ENDS_WITH_BY_KEY {
        @Override
        public Exp process(Map<String, Object> map) {
            String endWithRegexp = QualifierRegexpBuilder.getEndsWith(getValue1(map).toString());
            return Exp.regexCompare(endWithRegexp, regexFlags(map),
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getValue2(map).toString()), Exp.mapBin(getField(map)))
            );
        }
    },
    MAP_VALUE_CONTAINING_BY_KEY {
        @Override
        public Exp process(Map<String, Object> map) {
            String containingRegexp = QualifierRegexpBuilder.getContaining(getValue1(map).toString());
            return Exp.regexCompare(containingRegexp, regexFlags(map),
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getValue2(map).toString()), Exp.mapBin(getField(map)))
            );
        }
    },
    MAP_KEYS_CONTAINS {
        @Override
        public Exp process(Map<String, Object> map) {
            Exp exp;
            switch (getValue1(map).getType()) {
                case ParticleType.STRING:
                    exp = Exp.gt(
                            MapExp.getByKey(MapReturnType.COUNT, Exp.Type.INT, Exp.val(getValue2(map).toString()), Exp.mapBin(getField(map))),
                            Exp.val(0)
                    );
                    break;
                case ParticleType.INTEGER:
                    exp = Exp.gt(
                            MapExp.getByKey(MapReturnType.COUNT, Exp.Type.INT, Exp.val(getValue2(map).toLong()), Exp.mapBin(getField(map))),
                            Exp.val(0)
                    );
                    break;
                default:
                    throw new AerospikeException("FilterExpression unsupported type: expected String or Long (FilterOperation MAP_KEYS_CONTAINS)");
            }

            return exp;
        }
    },
    MAP_VALUES_CONTAINS {
        @Override
        public Exp process(Map<String, Object> map) {
            Exp exp;
            switch (getValue1(map).getType()) {
                case ParticleType.STRING:
                    exp = Exp.gt(
                            MapExp.getByValue(MapReturnType.COUNT, Exp.val(getValue2(map).toString()), Exp.mapBin(getField(map))),
                            Exp.val(0)
                    );
                    break;
                case ParticleType.INTEGER:
                    exp = Exp.gt(
                            MapExp.getByValue(MapReturnType.COUNT, Exp.val(getValue2(map).toLong()), Exp.mapBin(getField(map))),
                            Exp.val(0)
                    );
                    break;
                default:
                    throw new AerospikeException("FilterExpression unsupported type: expected String or Long (FilterOperation MAP_VALUES_CONTAINS)");
            }

            return exp;
        }
    },
    MAP_KEYS_BETWEEN {
        @Override
        public Exp process(Map<String, Object> map) {
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Exp.gt(
                        // + 1L to the valueEnd since the valueEnd is exclusive (both begin and values should be included).
                        MapExp.getByKeyRange(MapReturnType.COUNT, Exp.val(getValue1(map).toLong()), Exp.val(getValue2(map).toLong() + 1L), Exp.mapBin(getField(map))),
                        Exp.val(0)
                );
            }
            throw new AerospikeException("FilterExpression unsupported type: expected String or Long (FilterOperation MAP_KEYS_BETWEEN)");
        }
    },
    MAP_VALUES_BETWEEN {
        @Override
        public Exp process(Map<String, Object> map) {
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Exp.gt(
                        // + 1L to the valueEnd since the valueEnd is exclusive (both begin and values should be included).
                        MapExp.getByValueRange(MapReturnType.COUNT, Exp.val(getValue1(map).toLong()), Exp.val(getValue2(map).toLong() + 1L), Exp.mapBin(getField(map))),
                        Exp.val(0)
                );
            }
            throw new AerospikeException("FilterExpression unsupported type: expected String or Long (FilterOperation MAP_VALUES_BETWEEN)");
        }
    },
    GEO_WITHIN {
        @Override
        public Exp process(Map<String, Object> map) {
            return Exp.geoCompare(Exp.geoBin(getField(map)), Exp.geo(getValue1(map).toString()));
        }
    },
    LIST_CONTAINS {
        @Override
        public Exp process(Map<String, Object> map) {
            Exp exp;
            switch (getValue1(map).getType()) {
                case ParticleType.STRING:
                    exp = Exp.gt(
                            ListExp.getByValue(ListReturnType.COUNT, Exp.val(getValue1(map).toString()), Exp.listBin(getField(map))),
                            Exp.val(0)
                    );
                    break;
                case ParticleType.INTEGER:
                    exp = Exp.gt(
                            ListExp.getByValue(ListReturnType.COUNT, Exp.val(getValue1(map).toLong()), Exp.listBin(getField(map))),
                            Exp.val(0)
                    );
                    break;
                default:
                    throw new AerospikeException("FilterExpression unsupported type: expected String or Long (FilterOperation LIST_CONTAINS)");
            }

            return exp;
        }
    },
    LIST_VALUE_BETWEEN {
        @Override
        public Exp process(Map<String, Object> map) {
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Exp.gt(
                        // + 1L to the valueEnd since the valueEnd is exclusive (both begin and values should be included).
                        ListExp.getByValueRange(ListReturnType.COUNT, Exp.val(getValue1(map).toLong()), Exp.val(getValue2(map).toLong() + 1L), Exp.listBin(getField(map))),
                        Exp.val(0)
                );
            }
            throw new AerospikeException("FilterExpression unsupported type: expected String or Long (FilterOperation LIST_BETWEEN)");
        }
    },
    LIST_VALUE_GT {
        @Override
        public Exp process(Map<String, Object> map) {
            Exp exp;
            switch (getValue1(map).getType()) {
                case ParticleType.INTEGER:
                    exp = Exp.gt(
                            ListExp.getByValueRange(ListReturnType.COUNT, Exp.val(getValue1(map).toLong() + 1L), Exp.val(Long.MAX_VALUE), Exp.listBin(getField(map))),
                            Exp.val(0)
                            );
                    break;
                default:
                    throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation LIST_VALUE_GT)");
            };

            return exp;
        }
    },
    LIST_VALUE_GTEQ {
        @Override
        public Exp process(Map<String, Object> map) {
            Exp exp;
            switch (getValue1(map).getType()) {
                case ParticleType.INTEGER:
                    exp = Exp.gt(
                            ListExp.getByValueRange(ListReturnType.COUNT, Exp.val(getValue1(map).toLong()), Exp.val(Long.MAX_VALUE), Exp.listBin(getField(map))),
                            Exp.val(0)
                    );
                    break;
                default:
                    throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation LIST_VALUE_GT)");
            };

            return exp;
        }
    },
    LIST_VALUE_LT {
        @Override
        public Exp process(Map<String, Object> map) {
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Exp.gt(
                        ListExp.getByValueRange(ListReturnType.COUNT, Exp.val(Long.MIN_VALUE), Exp.val(getValue1(map).toLong() - 1L), Exp.listBin(getField(map))),
                        Exp.val(0)
                );
            }
            throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation LIST_VALUE_LT)");
        }
    },
    LIST_VALUE_LTEQ {
        @Override
        public Exp process(Map<String, Object> map) {
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Exp.gt(
                        ListExp.getByValueRange(ListReturnType.COUNT, Exp.val(Long.MIN_VALUE), Exp.val(getValue1(map).toLong()), Exp.listBin(getField(map))),
                        Exp.val(0)
                );
            }
            throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation LIST_VALUE_LTEQ)");
        }
    }
    ;

    public abstract Exp process(Map<String, Object> map);

    public String getField(Map<String, Object> map) {
        return (String) map.get(FIELD);
    }

    public Boolean ignoreCase(Map<String, Object> map) {
        return (Boolean) map.getOrDefault(IGNORE_CASE, false);
    }

    public int regexFlags (Map<String, Object> map) {
        return ignoreCase(map) ? RegexFlag.ICASE : RegexFlag.NONE;
    }

    public Value getValue1(Map<String, Object> map) {
        return (Value) map.get(VALUE1);
    }

    public Value getValue2(Map<String, Object> map) {
        return (Value) map.get(VALUE2);
    }

    public Value getValue3(Map<String, Object> map) {
        return (Value) map.get(VALUE3);
    }

}
