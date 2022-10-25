package org.springframework.data.aerospike.query;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.RegexFlag;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
        public Exp filterExp(Map<String, Object> map) { // Convert IN to a collection of or as Aerospike has no support for IN query
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

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return null;
        }
    },
    EQ {
        @Override
        public Exp filterExp(Map<String, Object> map) {
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

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Filter.equal(getField(map), getValue1(map).toLong());
            } else {
                // There is no case insensitive string comparison filter.
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
            int valType = val.getType();
            Exp exp;

            if (valType == ParticleType.INTEGER) {
                exp = Exp.ne(Exp.intBin(getField(map)), Exp.val(val.toLong()));
            } else {
                exp = Exp.ne(Exp.stringBin(getField(map)), Exp.val(val.toString()));
            }
            return exp;
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return null; // not supported in the secondary index filter
        }
    },
    GT {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Exp.gt(Exp.intBin(getField(map)), Exp.val(getValue1(map).toLong()));
            }
            throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation GT)");
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            // Long.MAX_VALUE shall not be given as + 1 will cause overflow
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue1(map).toLong() == Long.MAX_VALUE) {
                throw new AerospikeException("sIndexFilter unsupported type: expected [Long.MIN_VALUE..Long.MAX_VALUE-1] (FilterOperation GT)");
            }

            return Filter.range(getField(map), getValue1(map).toLong() + 1, Long.MAX_VALUE);
        }
    },
    GTEQ {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Exp.ge(Exp.intBin(getField(map)), Exp.val(getValue1(map).toLong()));
            }
            throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation GTEQ)");
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException("sIndexFilter unsupported type: expected Long (FilterOperation GTEQ)");
            }
            return Filter.range(getField(map), getValue1(map).toLong(), Long.MAX_VALUE);
        }
    },
    LT {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Exp.lt(Exp.intBin(getField(map)), Exp.val(getValue1(map).toLong()));
            }
            throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation LT)");
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            // Long.MIN_VALUE shall not be given as - 1 will cause overflow
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue1(map).toLong() == Long.MIN_VALUE) {
                throw new AerospikeException("sIndexFilter unsupported type: expected [Long.MIN_VALUE+1..Long.MAX_VALUE] (FilterOperation LT)");
            }
            return Filter.range(getField(map), Long.MIN_VALUE, getValue1(map).toLong() - 1);
        }
    },
    LTEQ {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Exp.le(Exp.intBin(getField(map)), Exp.val(getValue1(map).toLong()));
            }
            throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation LTEQ)");
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException("sIndexFilter unsupported type: expected Long (FilterOperation LTEQ)");
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
            throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation BETWEEN)");
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue2(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException("sIndexFilter unsupported type: expected Long (FilterOperation BETWEEN)");
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
            }

            return exp;
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            Filter filter;

            switch (getValue1(map).getType()) {
                case ParticleType.STRING:
                    // There is no case insensitive string comparison filter.
                    if (ignoreCase(map)) return null;
                    filter = Filter.contains(getField(map), IndexCollectionType.MAPVALUES, getValue1(map).toString());
                    break;
                case ParticleType.INTEGER:
                    filter = Filter.range(getField(map), IndexCollectionType.MAPVALUES, getValue1(map).toLong(), getValue1(map).toLong());
                    break;
                default:
                    throw new AerospikeException("FilterExpression unsupported type: expected String or Long (FilterOperation MAP_VALUE_EQ_BY_KEY)");
            }

            return filter;
        }
    },
    MAP_VALUE_NOTEQ_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
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

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return null; // not supported in the secondary index filter
        }
    },
    MAP_VALUE_GT_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            // VALUE2 contains key (field name)
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Exp.gt(
                        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2(map).toString()), Exp.mapBin(getField(map))),
                        Exp.val(getValue1(map).toLong())
                );
            }
            throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation MAP_VALUE_GT_BY_KEY)");
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            // Long.MAX_VALUE shall not be given as + 1 will cause overflow
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue1(map).toLong() == Long.MAX_VALUE) {
                throw new AerospikeException("sIndexFilter unsupported type: expected [Long.MIN_VALUE..Long.MAX_VALUE-1] (FilterOperation MAP_VALUE_GT_BY_KEY)");
            }

            return Filter.range(getField(map), IndexCollectionType.MAPVALUES, getValue1(map).toLong() + 1, Long.MAX_VALUE);
        }
    },
    MAP_VALUE_GTEQ_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            // VALUE2 contains key (field name)
            if (getValue1(map).getType() == ParticleType.INTEGER) {
                return Exp.ge(
                        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2(map).toString()), Exp.mapBin(getField(map))),
                        Exp.val(getValue1(map).toLong())
                );
            }
            throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation MAP_VALUE_GTEQ_BY_KEY)");
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException("sIndexFilter unsupported type: expected Long (FilterOperation MAP_VALUE_GTEQ_BY_KEY)");
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
                        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2(map).toString()), Exp.mapBin(getField(map))),
                        Exp.val(getValue1(map).toLong())
                );
            }
            throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation MAP_VALUE_LT_BY_KEY)");
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            // Long.MIN_VALUE shall not be given as - 1 will cause overflow
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue1(map).toLong() == Long.MIN_VALUE) {
                throw new AerospikeException("sIndexFilter unsupported type: expected [Long.MIN_VALUE+1..Long.MAX_VALUE] (FilterOperation MAP_VALUE_LT_BY_KEY)");
            }

            return Filter.range(getField(map), IndexCollectionType.MAPVALUES, Long.MIN_VALUE, getValue1(map).toLong() - 1);
        }
    },
    MAP_VALUE_LTEQ_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            // VALUE2 contains key (field name)
            if (getValue1(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation MAP_VALUE_LTEQ_BY_KEY)");
            }

            return Exp.le(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2(map).toString()), Exp.mapBin(getField(map))),
                    Exp.val(getValue1(map).toLong())
            );
        }

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException("sIndexFilter unsupported type: expected Long (FilterOperation MAP_VALUE_LTEQ_BY_KEY)");
            }

            return Filter.range(getField(map), IndexCollectionType.MAPVALUES, Long.MIN_VALUE, getValue1(map).toLong());
        }
    },
    MAP_VALUES_BETWEEN_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            // VALUE2 contains key (field name), VALUE3 contains upper limit
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue3(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation MAP_VALUES_BETWEEN_BY_KEY)");
            }

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

        /**
         * This secondary index filter must NOT run independently as it requires also FilterExpression to filter by key
         */
        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue3(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException("sIndex filter unsupported type: expected Long (FilterOperation MAP_VALUES_BETWEEN_BY_KEY)");
            }

            return Filter.range(getField(map), IndexCollectionType.MAPVALUES, getValue1(map).toLong(), getValue3(map).toLong());
        }
    },
    MAP_VALUE_STARTS_WITH_BY_KEY {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            String startWithRegexp = QualifierRegexpBuilder.getStartsWith(getValue1(map).toString());

            return Exp.regexCompare(startWithRegexp, regexFlags(map),
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getValue2(map).toString()), Exp.mapBin(getField(map)))
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
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getValue2(map).toString()), Exp.mapBin(getField(map)))
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
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getValue2(map).toString()), Exp.mapBin(getField(map)))
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
            Exp exp;
            switch (getValue1(map).getType()) {
                case ParticleType.STRING:
                    exp = Exp.gt(
                            MapExp.getByKey(MapReturnType.COUNT, Exp.Type.INT, Exp.val(getValue1(map).toString()), Exp.mapBin(getField(map))),
                            Exp.val(0)
                    );
                    break;
                case ParticleType.INTEGER:
                    exp = Exp.gt(
                            MapExp.getByKey(MapReturnType.COUNT, Exp.Type.INT, Exp.val(getValue1(map).toLong()), Exp.mapBin(getField(map))),
                            Exp.val(0)
                    );
                    break;
                default:
                    throw new AerospikeException("FilterExpression unsupported type: expected String or Long (FilterOperation MAP_KEYS_CONTAINS)");
            }

            return exp;
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            return collectionContains(IndexCollectionType.MAPKEYS, map);
        }
    },
    MAP_VALUES_CONTAIN {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            Exp exp;
            switch (getValue1(map).getType()) {
                case ParticleType.STRING:
                    exp = Exp.gt(
                            MapExp.getByValue(MapReturnType.COUNT, Exp.val(getValue1(map).toString()), Exp.mapBin(getField(map))),
                            Exp.val(0)
                    );
                    break;
                case ParticleType.INTEGER:
                    exp = Exp.gt(
                            MapExp.getByValue(MapReturnType.COUNT, Exp.val(getValue1(map).toLong()), Exp.mapBin(getField(map))),
                            Exp.val(0)
                    );
                    break;
                default:
                    throw new AerospikeException("FilterExpression unsupported type: expected String or Long (FilterOperation MAP_VALUES_CONTAINS)");
            }

            return exp;
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
                throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation MAP_KEYS_BETWEEN)");
            }

            // + 1L to the valueEnd since the valueEnd is exclusive
            Exp upperLimit = Exp.val(getValue2(map).toLong() + 1L);

            // Long.MAX_VALUE will not be processed correctly if given as an inclusive parameter as it will cause overflow
            if (getValue2(map).toLong() == Long.MAX_VALUE) upperLimit = null;


            return Exp.gt(
                    // + 1L to the valueEnd since the valueEnd is exclusive
                    MapExp.getByKeyRange(MapReturnType.COUNT, Exp.val(getValue1(map).toLong()), upperLimit, Exp.mapBin(getField(map))),
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
                throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation MAP_VALUES_BETWEEN)");
            }

            // + 1L to the valueEnd since the valueEnd is exclusive (both begin and values should be included).
            Exp upperLimit = Exp.val(getValue2(map).toLong() + 1L);

            // Long.MAX_VALUE will not be processed correctly if given as an inclusive parameter as it will cause overflow
            if (getValue2(map).toLong() == Long.MAX_VALUE) upperLimit = null;


            return Exp.gt(
                    // + 1L to the valueEnd since the valueEnd is exclusive
                    MapExp.getByValueRange(MapReturnType.COUNT, Exp.val(getValue1(map).toLong()), upperLimit, Exp.mapBin(getField(map))),
                    Exp.val(0)
            );
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue2(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException("sIndex filter unsupported type: expected Long (FilterOperation MAP_VALUES_BETWEEN)");
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

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.STRING && getValue1(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException("sIndexFilter unsupported type: expected String or Long (FilterOperation LIST_CONTAINS)");
            }

            return collectionContains(IndexCollectionType.LIST, map);
        }
    },
    LIST_VALUE_BETWEEN {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue2(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation LIST_BETWEEN)");
            }

            // + 1L to the valueEnd since the valueEnd is exclusive
            Exp upperLimit = Exp.val(getValue2(map).toLong() + 1L);

            // Long.MAX_VALUE will not be processed correctly if given an inclusive parameter as it will cause overflow
            if (getValue2(map).toLong() == Long.MAX_VALUE) upperLimit = null;

            return Exp.gt(
                    ListExp.getByValueRange(ListReturnType.COUNT, Exp.val(getValue1(map).toLong()), upperLimit, Exp.listBin(getField(map))),
                    Exp.val(0)
            );
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue2(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException("sIndexFilter unsupported type: expected Long (FilterOperation LIST_BETWEEN)");
            }

            return collectionRange(IndexCollectionType.LIST, map);
        }
    },
    LIST_VALUE_GT {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue1(map).toLong() == Long.MAX_VALUE) {
                throw new AerospikeException("FilterExpression unsupported type: expected [Long.MIN_VALUE..Long.MAX_VALUE-1] (FilterOperation LIST_VALUE_GT)");
            }

            return Exp.gt(
                    ListExp.getByValueRange(ListReturnType.COUNT, Exp.val(getValue1(map).toLong() + 1L), Exp.val(Long.MAX_VALUE), Exp.listBin(getField(map))),
                    Exp.val(0)
            );
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            // Long.MAX_VALUE shall not be given as + 1 will cause overflow
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue1(map).toLong() == Long.MAX_VALUE) {
                throw new AerospikeException("sIndexFilter unsupported type: expected [Long.MIN_VALUE..Long.MAX_VALUE-1] (FilterOperation LIST_VALUE_GT)");
            }

            return Filter.range(getField(map), IndexCollectionType.LIST, getValue1(map).toLong() + 1, Long.MAX_VALUE);
        }
    },
    LIST_VALUE_GTEQ {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation LIST_VALUE_GTEQ)");
            }

            return Exp.gt(
                    ListExp.getByValueRange(ListReturnType.COUNT, Exp.val(getValue1(map).toLong()), null, Exp.listBin(getField(map))),
                    Exp.val(0)
            );
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException("sIndexFilter unsupported type: expected Long (FilterOperation LIST_VALUE_GTEQ)");
            }

            return Filter.range(getField(map), IndexCollectionType.LIST, getValue1(map).toLong(), Long.MAX_VALUE);
        }
    },
    LIST_VALUE_LT {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue1(map).toLong() == Long.MIN_VALUE) {
                throw new AerospikeException("FilterExpression unsupported type: expected [Long.MIN_VALUE+1..Long.MAX_VALUE] (FilterOperation LIST_VALUE_LT)");
            }

            return Exp.gt(
                    ListExp.getByValueRange(ListReturnType.COUNT, Exp.val(Long.MIN_VALUE), Exp.val(getValue1(map).toLong() - 1L), Exp.listBin(getField(map))),
                    Exp.val(0)
            );
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            // Long.MIN_VALUE shall not be given as - 1 will cause overflow
            if (getValue1(map).getType() != ParticleType.INTEGER || getValue1(map).toLong() == Long.MIN_VALUE) {
                throw new AerospikeException("sIndexFilter unsupported type: expected [Long.MIN_VALUE+1..Long.MAX_VALUE] (FilterOperation LIST_VALUE_LT)");
            }

            return Filter.range(getField(map), IndexCollectionType.LIST, Long.MIN_VALUE, getValue1(map).toLong() - 1);
        }
    },
    LIST_VALUE_LTEQ {
        @Override
        public Exp filterExp(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException("FilterExpression unsupported type: expected Long (FilterOperation LIST_VALUE_LTEQ)");
            }

            // + 1L to the valueEnd since the valueEnd is exclusive
            Exp upperLimit = Exp.val(getValue1(map).toLong() + 1L);

            // Long.MIN_VALUE will not be processed correctly as - 1 will cause overflow
            if (getValue1(map).toLong() == Long.MAX_VALUE) upperLimit = null;

            return Exp.gt(
                    ListExp.getByValueRange(ListReturnType.COUNT, Exp.val(Long.MIN_VALUE), upperLimit, Exp.listBin(getField(map))),
                    Exp.val(0)
            );
        }

        @Override
        public Filter sIndexFilter(Map<String, Object> map) {
            if (getValue1(map).getType() != ParticleType.INTEGER) {
                throw new AerospikeException("sIndexFilter unsupported type: expected Long (FilterOperation LIST_VALUE_LTEQ)");
            }

            return Filter.range(getField(map), IndexCollectionType.LIST, Long.MIN_VALUE, getValue1(map).toLong());
        }
    };

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

    protected Filter collectionContains(IndexCollectionType collectionType, Map<String, Object> map) {
        Value val = getValue1(map);
        int valType = val.getType();
        switch (valType) {
            case ParticleType.INTEGER:
                return Filter.contains(getField(map), collectionType, val.toLong());
            case ParticleType.STRING:
                return Filter.contains(getField(map), collectionType, val.toString());
        }
        return null;
    }

    protected Filter collectionRange(IndexCollectionType collectionType, Map<String, Object> map) {
        return Filter.range(getField(map), collectionType, getValue1(map).toLong(), getValue2(map).toLong());
    }

    protected Filter geoWithinRadius(IndexCollectionType collectionType, Map<String, Object> map) {
        return Filter.geoContains(getField(map), getValue1(map).toString());
    }

    /**
     * FilterOperations that require both sIndexFilter and FilterExpression
     */
    public static final List<FilterOperation> dualFilterOperations = Arrays.asList(
            MAP_VALUE_EQ_BY_KEY, MAP_VALUE_GT_BY_KEY, MAP_VALUE_GTEQ_BY_KEY, MAP_VALUE_LT_BY_KEY, MAP_VALUE_LTEQ_BY_KEY, MAP_VALUES_BETWEEN_BY_KEY
    );
}
