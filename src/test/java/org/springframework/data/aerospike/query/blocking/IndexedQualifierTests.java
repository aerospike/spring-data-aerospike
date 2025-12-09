/*
 * Copyright 2012-2020 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.springframework.data.aerospike.query.blocking;

import com.aerospike.client.Value;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.annotation.Extensive;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.KeyRecordIterator;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.util.AwaitilityUtils;
import org.springframework.data.aerospike.util.CollectionUtils;

import java.util.Map;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.AGES;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.BLUE;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.GEO_BIN_NAME;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.GREEN;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.INDEXED_GEO_SET;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.INDEXED_SET_NAME;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.ORANGE;
import static org.springframework.data.aerospike.util.CollectionUtils.countingInt;

/*
 * These tests generate qualifiers on indexed bins.
 */
@Extensive
class IndexedQualifierTests extends BaseQueryEngineTests {

    @AfterEach
    public void assertNoScans() {
        try {
            additionalAerospikeTestOperations.assertNoScansForSet(INDEXED_SET_NAME);
        } catch (Exception e) {
            AwaitilityUtils.wait(2000, MILLISECONDS);
            additionalAerospikeTestOperations.assertNoScansForSet(INDEXED_SET_NAME);
        }
    }

    @Test
    void selectOnIndexedLTQualifier() {
        withIndex(namespace, INDEXED_SET_NAME, "age_index", "age", IndexType.NUMERIC, () -> {
            // Ages range from 25 -> 29. We expected to only get back values with age < 26
            Qualifier ageLt26 = Qualifier.builder()
                .setPath("age")
                .setFilterOperation(FilterOperation.LT)
                .setValue(26)
                .build();

            KeyRecordIterator iterator = queryEngine.select(namespace, INDEXED_SET_NAME, null,
                new Query(ageLt26));

            assertThat(iterator)
                .toIterable()
                .isNotEmpty()
                .allSatisfy(rec -> assertThat(rec.record.getInt("age")).isLessThan(26))
                .hasSize(queryEngineTestDataPopulator.ageCount.get(25));
        });
    }

    @Test
    void selectOnIndexedLTEQQualifier() {
        withIndex(namespace, INDEXED_SET_NAME, "age_index", "age", IndexType.NUMERIC, () -> {
            // Ages range from 25 -> 29. We expected to only get back values with age <= 26
            Qualifier ageLteq26 = Qualifier.builder()
                .setPath("age")
                .setFilterOperation(FilterOperation.LTEQ)
                .setValue(26)
                .build();

            KeyRecordIterator iterator = queryEngine.select(namespace, INDEXED_SET_NAME, null,
                new Query(ageLteq26));

            Map<Integer, Integer> ageCount = CollectionUtils.toStream(iterator)
                .map(rec -> rec.record.getInt("age"))
                .collect(Collectors.groupingBy(k -> k, countingInt()));
            assertThat(ageCount.keySet())
                .isNotEmpty()
                .allSatisfy(age -> assertThat(age).isLessThanOrEqualTo(26));
            assertThat(ageCount.get(25)).isEqualTo(queryEngineTestDataPopulator.ageCount.get(25));
            assertThat(ageCount.get(26)).isEqualTo(queryEngineTestDataPopulator.ageCount.get(26));
        });
    }

    @Test
    void selectOnIndexedNumericEQQualifier() {
        withIndex(namespace, INDEXED_SET_NAME, "age_index", "age", IndexType.NUMERIC, () -> {
            // Ages range from 25 -> 29. We expected to only get back values with age == 26
            Qualifier ageEq26 = Qualifier.builder()
                .setPath("age")
                .setFilterOperation(FilterOperation.EQ)
                .setValue(26)
                .build();

            KeyRecordIterator iterator = queryEngine.select(namespace, INDEXED_SET_NAME, null,
                new Query(ageEq26));

            assertThat(iterator)
                .toIterable()
                .isNotEmpty()
                .allSatisfy(rec -> assertThat(rec.record.getInt("age")).isEqualTo(26))
                .hasSize(queryEngineTestDataPopulator.ageCount.get(26));
        });
    }

    @Test
    void selectOnIndexedBlueColorQuery_withIndexOnDifferentBin() {
        withIndex(namespace, INDEXED_SET_NAME, "age_index", "age", IndexType.NUMERIC, () -> {
            Qualifier colorEqBlue = Qualifier.builder()
                .setPath("color")
                .setFilterOperation(FilterOperation.EQ)
                .setValue(BLUE)
                .build();

            assertQueryHasNoSecIndexFilter(new Query(colorEqBlue), KeyRecord.class);
            KeyRecordIterator it = queryEngine.select(namespace, INDEXED_SET_NAME, new Query(colorEqBlue));

            assertThat(it)
                .toIterable()
                .isNotEmpty()
                .allSatisfy(rec -> assertThat(rec.record.getInt("age")).isBetween(25, 29))
                .hasSize(queryEngineTestDataPopulator.colourCounts.get(BLUE));
        });
    }

    @Test
    void selectOnIndexedBlueColorQuery_withDslExpression() {
        withIndex(namespace, INDEXED_SET_NAME, "color_index", "color", IndexType.STRING, () -> {
            Qualifier colorEqBlue = Qualifier.dslExpressionBuilder()
                // Using a static DSL expression with a fixed value
                .setDSLExpressionString("$.color == '" + BLUE + "'")
                .build();

            assertQueryHasSecIndexFilter(new Query(colorEqBlue), KeyRecord.class);
            KeyRecordIterator it = queryEngine.select(namespace, INDEXED_SET_NAME, new Query(colorEqBlue));

            assertThat(it)
                .toIterable()
                .isNotEmpty()
                .allSatisfy(rec -> assertThat(rec.record.getInt("age")).isBetween(25, 29))
                .hasSize(queryEngineTestDataPopulator.colourCounts.get(BLUE));
        });
    }

    @Test
    void selectOnIndexedGTEQQualifier() {
        withIndex(namespace, INDEXED_SET_NAME, "age_index", "age", IndexType.NUMERIC, () -> {
            // Ages range from 25 -> 29. We expected to only get back values with age >= 28
            Qualifier ageGteq28 = Qualifier.builder()
                .setPath("age")
                .setFilterOperation(FilterOperation.GTEQ)
                .setValue(28)
                .build();

            KeyRecordIterator iterator = queryEngine.select(namespace, INDEXED_SET_NAME, null,
                new Query(ageGteq28));

            Map<Integer, Integer> ageCount = CollectionUtils.toStream(iterator)
                .map(rec -> rec.record.getInt("age"))
                .collect(Collectors.groupingBy(k -> k, countingInt()));
            assertThat(ageCount.keySet())
                .isNotEmpty()
                .allSatisfy(age -> assertThat(age).isGreaterThanOrEqualTo(28));
            assertThat(ageCount.get(28)).isEqualTo(queryEngineTestDataPopulator.ageCount.get(28));
            assertThat(ageCount.get(29)).isEqualTo(queryEngineTestDataPopulator.ageCount.get(29));
        });
    }

    @Test
    void selectOnIndexedGTQualifier() {
        withIndex(namespace, INDEXED_SET_NAME, "age_index", "age", IndexType.NUMERIC, () -> {
            Qualifier ageGt28 = Qualifier.builder()
                .setPath("age")
                .setFilterOperation(FilterOperation.GT)
                .setValue(28)
                .build();

            KeyRecordIterator iterator = queryEngine.select(namespace, INDEXED_SET_NAME, null,
                new Query(ageGt28));

            assertThat(iterator)
                .toIterable()
                .isNotEmpty()
                .allSatisfy(rec -> assertThat(rec.record.getInt("age")).isEqualTo(29))
                .hasSize(queryEngineTestDataPopulator.ageCount.get(29));
        });
    }

    @Test
    void selectOnIndexedStringEQQualifier() {
        withIndex(namespace, INDEXED_SET_NAME, "color_index", "color", IndexType.STRING, () -> {
            Qualifier colorEqOrange = Qualifier.builder()
                .setPath("color")
                .setFilterOperation(FilterOperation.EQ)
                .setValue(ORANGE)
                .build();

            KeyRecordIterator iterator = queryEngine.select(namespace, INDEXED_SET_NAME, null,
                new Query(colorEqOrange));

            assertThat(iterator)
                .toIterable()
                .isNotEmpty()
                .allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(ORANGE))
                .hasSize(queryEngineTestDataPopulator.colourCounts.get(ORANGE));
        });
    }

    @Test
    void selectOnIndexedGeoWithinQualifier() {
        if (serverVersionSupport.isDropCreateBehaviorUpdated()) {
            withIndex(namespace, INDEXED_GEO_SET, "geo_index", GEO_BIN_NAME, IndexType.GEO2DSPHERE, () -> {
                double lon = -122.0;
                double lat = 37.5;
                double radius = 50000.0;
                String rgnstr = String.format("{ \"type\": \"AeroCircle\", "
                        + "\"coordinates\": [[%.8f, %.8f], %f] }",
                    lon, lat, radius);

                Qualifier geoWithin = Qualifier.builder()
                    .setPath(GEO_BIN_NAME)
                    .setFilterOperation(FilterOperation.GEO_WITHIN)
                    .setValue(Value.getAsGeoJSON(rgnstr))
                    .build();

                KeyRecordIterator iterator = queryEngine.select(namespace, INDEXED_GEO_SET, null,
                    new Query(geoWithin));

                assertThat(iterator).toIterable()
                    .isNotEmpty()
                    .allSatisfy(rec -> assertThat(rec.record.generation).isPositive());
                additionalAerospikeTestOperations.assertNoScansForSet(INDEXED_GEO_SET);
            });
        }
    }

    @Test
    void selectWithoutQuery() {
        withIndex(namespace, INDEXED_SET_NAME, "age_index", "age", IndexType.NUMERIC, () -> {
            KeyRecordIterator it = queryEngine.select(namespace, INDEXED_SET_NAME, null);

            Map<Integer, Integer> ageCount = CollectionUtils.toStream(it)
                .map(rec -> rec.record.getInt("age"))
                .collect(Collectors.groupingBy(k -> k, countingInt()));
            assertThat(ageCount.keySet())
                .isNotEmpty()
                .allSatisfy(age -> assertThat(age).isIn((Object[]) AGES));
            assertThat(ageCount.get(28)).isEqualTo(queryEngineTestDataPopulator.ageCount.get(28));
            assertThat(ageCount.get(29)).isEqualTo(queryEngineTestDataPopulator.ageCount.get(29));
        });
    }

    @Test
    void selectOnIndexedAndQualifier_withOneIndex() {
        Qualifier colorEqGreen = Qualifier.builder()
            .setPath("color")
            .setFilterOperation(FilterOperation.EQ)
            .setValue(GREEN)
            .build();
        Qualifier ageBetween28And29 = Qualifier.builder()
            .setPath("age")
            .setFilterOperation(FilterOperation.BETWEEN)
            .setValue(28)
            .setSecondValue(29)
            .build();

        withIndex(namespace, INDEXED_SET_NAME, "age_index", "age", IndexType.NUMERIC, () -> {
            KeyRecordIterator it = queryEngine.select(namespace, INDEXED_SET_NAME, null,
                new Query(Qualifier.and(colorEqGreen, ageBetween28And29)));

            assertThat(it).toIterable()
                .isNotEmpty()
                .allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(GREEN))
                .allSatisfy(rec -> assertThat(rec.record.getInt("age")).isBetween(28, 29));
        });
    }

    @Test
    void selectOnIndexedAndQualifier_withOneIndex_usingDslExpression() {
        Qualifier colorAndAge = Qualifier.dslExpressionBuilder()
            // Using a comprehensive static DSL expression with fixed values
            .setDSLExpressionString("$.color == '" + GREEN + "' and $.age >= 28 and $.age < 29")
            .build();
        Query query = new Query(colorAndAge);

        withIndex(namespace, INDEXED_SET_NAME, "age_index", "age", IndexType.NUMERIC, () -> {
            // Assert that the query has both a secondary index Filter and a filtering Expression
            assertQueryHasSecIndexFilter(query, KeyRecord.class);
            assertThat(query.getCriteriaObject().hasFilterExpression()).isTrue();

            KeyRecordIterator it = queryEngine.select(namespace, INDEXED_SET_NAME, null, query);

            assertThat(it).toIterable()
                .isNotEmpty()
                .allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(GREEN))
                .allSatisfy(rec -> assertThat(rec.record.getInt("age")).isBetween(28, 29));
        });

        // It is currently NOT allowed to combine qualifiers if at least one of them is a DSL expression qualifier
        Qualifier colorEqGreen = Qualifier.dslExpressionBuilder()
            // Using a static DSL expression with a fixed value
            .setDSLExpressionString("$.color == '" + GREEN + "'")
            .build();
        Qualifier ageBetween28And29 = Qualifier.builder()
            .setPath("age")
            .setFilterOperation(FilterOperation.BETWEEN)
            .setValue(28)
            .setSecondValue(29)
            .build();

        assertThatThrownBy(() -> new Query(Qualifier.and(colorEqGreen, ageBetween28And29)))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Cannot combine DSL expression qualifiers in a custom logical query, " +
                "please incorporate all conditions into one comprehensive DSL expression or combine non-DSL " +
                "qualifiers");

        Qualifier ageBetween28And29_DslExpr = Qualifier.dslExpressionBuilder()
            // Using a static DSL expression with fixed values
            .setDSLExpressionString("$.age > 28 and $.age <= 29")
            .build();

        assertThatThrownBy(() -> new Query(Qualifier.and(colorEqGreen, colorEqGreen, ageBetween28And29_DslExpr)))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Cannot combine DSL expression qualifiers in a custom logical query, " +
                "please incorporate all conditions into one comprehensive DSL expression or combine non-DSL " +
                "qualifiers");
    }

    @Test
    void selectOnIndexedAndQualifier_withTwoIndexes() {
        Qualifier colorEqGreen = Qualifier.builder()
            .setPath("color")
            .setFilterOperation(FilterOperation.EQ)
            .setValue(GREEN)
            .build();
        Qualifier ageBetween28And29 = Qualifier.builder()
            .setPath("age")
            .setFilterOperation(FilterOperation.BETWEEN)
            .setValue(28)
            .setSecondValue(29)
            .build();

        tryCreateIndex(namespace, INDEXED_SET_NAME, "age_index", "age", IndexType.NUMERIC);
        tryCreateIndex(namespace, INDEXED_SET_NAME, "color_index", "color", IndexType.STRING);
        try {
            Qualifier qualifier = Qualifier.and(colorEqGreen, ageBetween28And29);

            KeyRecordIterator it = queryEngine.select(namespace, INDEXED_SET_NAME, null, new Query(qualifier));

            assertThat(it).toIterable().isNotEmpty()
                .allSatisfy(rec -> {
                    assertThat(rec.record.getInt("age")).isBetween(28, 29);
                    assertThat(rec.record.getString("color")).isEqualTo(GREEN);
                });
        } finally {
            tryDropIndex(INDEXED_SET_NAME, "age_index");
            tryDropIndex(INDEXED_SET_NAME, "color_index");
        }
    }

    @Test
    void selectOnIndexedAndQualifier_withTwoIndexes_usingDslExpression() {
        // Combining in one DSL expression
        Qualifier colorAndAge = Qualifier.dslExpressionBuilder()
            // Using a static DSL expression with fixed values
            .setDSLExpressionString("$.color == '" + GREEN + "' and $.age >= 28 and $.age < 29")
            .build();

        Query query = new Query(colorAndAge);
        tryCreateIndex(namespace, INDEXED_SET_NAME, "age_index", "age", IndexType.NUMERIC);
        tryCreateIndex(namespace, INDEXED_SET_NAME, "color_index", "color", IndexType.STRING);
        // An index for a non-existing bin
        tryCreateIndex(namespace, INDEXED_SET_NAME, "some_bin_index", "some_bin", IndexType.STRING);

        // Assert that the query has both a secondary index Filter and a filtering Expression
        assertQueryHasSecIndexFilter(query, KeyRecord.class);
        assertThat(query.getCriteriaObject().getFilterExpression()).isEqualTo(Exp.build(
            Exp.and(
                Exp.eq(Exp.bin("color", Exp.Type.STRING), Exp.val(GREEN)),
                Exp.lt(Exp.bin("age", Exp.Type.INT), Exp.val(29))
            )
        ));

        // When there are multiple indexes available for bins in a complex DSL expression query, the index
        // for secondary index Filter is chosen by cardinality (preferring indexes with a higher `binValuesRatio`).
        // If cardinality of indexes is the same, then one is chosen based on alphabetical order (like in this test)
        assertThat(getQuerySecIndexFilter(query, KeyRecord.class))
            .isEqualTo(Filter.range("age", 28, Long.MAX_VALUE));

        try {
            KeyRecordIterator it = queryEngine.select(namespace, INDEXED_SET_NAME, null, query);

            assertThat(it).toIterable().isNotEmpty()
                .allSatisfy(rec -> {
                    assertThat(rec.record.getInt("age")).isBetween(28, 29);
                    assertThat(rec.record.getString("color")).isEqualTo(GREEN);
                });
        } finally {
            tryDropIndex(INDEXED_SET_NAME, "age_index");
            tryDropIndex(INDEXED_SET_NAME, "color_index");
            tryDropIndex(INDEXED_SET_NAME, "some_bin_index");
        }
    }

    @Test
    void selectOnIndexedAndQualifier_withTwoIndexes_usingDslExpression_selectingIndexToUse() {
        // Combining in one DSL expression
        Qualifier colorAndAge = Qualifier.dslExpressionBuilder()
            // Using a static DSL expression with fixed values
            .setDSLExpressionString("$.color == '" + GREEN + "' and $.age >= 28 and $.age < 29")
            .setDSLExpressionIndexToUse("color_index")
            .build();

        Query query = new Query(colorAndAge);
        tryCreateIndex(namespace, INDEXED_SET_NAME, "age_index", "age", IndexType.NUMERIC);
        tryCreateIndex(namespace, INDEXED_SET_NAME, "color_index", "color", IndexType.STRING);
        // An index for a non-existing bin
        tryCreateIndex(namespace, INDEXED_SET_NAME, "some_bin_index", "some_bin", IndexType.STRING);

        // Assert that the query has both a secondary index Filter and a filtering Expression
        assertQueryHasSecIndexFilter(query, KeyRecord.class);
        assertThat(query.getCriteriaObject().getFilterExpression()).isEqualTo(Exp.build(
            Exp.and(
                Exp.ge(Exp.bin("age", Exp.Type.INT), Exp.val(28)),
                Exp.lt(Exp.bin("age", Exp.Type.INT), Exp.val(29))
            )
        ));

        // The index for building a Filter is manually chosen in this case
        assertThat(getQuerySecIndexFilter(query, KeyRecord.class))
            .isEqualTo(Filter.equal("color", GREEN));

        try {
            KeyRecordIterator it = queryEngine.select(namespace, INDEXED_SET_NAME, null, query);

            assertThat(it).toIterable().isNotEmpty()
                .allSatisfy(rec -> {
                    assertThat(rec.record.getInt("age")).isBetween(28, 29);
                    assertThat(rec.record.getString("color")).isEqualTo(GREEN);
                });
        } finally {
            tryDropIndex(INDEXED_SET_NAME, "age_index");
            tryDropIndex(INDEXED_SET_NAME, "color_index");
            tryDropIndex(INDEXED_SET_NAME, "some_bin_index");
        }
    }

    @Test
    void selectOnIndexedAndQualifier_withTwoIndexes_usingDslExpression_selectingNonExistingIndexToUse() {
        // Combining in one DSL expression
        Qualifier colorAndAge = Qualifier.dslExpressionBuilder()
            // Using a static DSL expression with fixed values
            .setDSLExpressionString("$.color == '" + GREEN + "' and $.age >= 28 and $.age < 29")
            .setDSLExpressionIndexToUse("color_index2") // There is no such index in cache
            .build();

        Query query = new Query(colorAndAge);
        tryCreateIndex(namespace, INDEXED_SET_NAME, "age_index", "age", IndexType.NUMERIC);
        tryCreateIndex(namespace, INDEXED_SET_NAME, "color_index", "color", IndexType.STRING);
        // An index for a non-existing bin
        tryCreateIndex(namespace, INDEXED_SET_NAME, "some_bin_index", "some_bin", IndexType.STRING);

        // Assert that the query has both a secondary index Filter and a filtering Expression
        assertQueryHasSecIndexFilter(query, KeyRecord.class);
        assertThat(query.getCriteriaObject().getFilterExpression()).isEqualTo(Exp.build(
            Exp.and(
                Exp.eq(Exp.bin("color", Exp.Type.STRING), Exp.val(GREEN)),
                Exp.lt(Exp.bin("age", Exp.Type.INT), Exp.val(29))
            )
        ));

        // When there are multiple indexes available for bins in a complex DSL expression query, the index
        // for secondary index Filter is chosen by cardinality (preferring indexes with a higher `binValuesRatio`).
        // If cardinality of indexes is the same, then one is chosen based on alphabetical order (like in this test)
        assertThat(getQuerySecIndexFilter(query, KeyRecord.class))
            .isEqualTo(Filter.range("age", 28, Long.MAX_VALUE));

        try {
            KeyRecordIterator it = queryEngine.select(namespace, INDEXED_SET_NAME, null, query);

            assertThat(it).toIterable().isNotEmpty()
                .allSatisfy(rec -> {
                    assertThat(rec.record.getInt("age")).isBetween(28, 29);
                    assertThat(rec.record.getString("color")).isEqualTo(GREEN);
                });
        } finally {
            tryDropIndex(INDEXED_SET_NAME, "age_index");
            tryDropIndex(INDEXED_SET_NAME, "color_index");
            tryDropIndex(INDEXED_SET_NAME, "some_bin_index");
        }
    }

    @Test
    void selectOnIndexedOrQualifier_withTwoIndexes_usingDslExpression() {
        // Combining in one DSL expression
        Qualifier colorAndAge = Qualifier.dslExpressionBuilder()
            // Using a static DSL expression with fixed values
            .setDSLExpressionString("$.color == '" + GREEN + "' or ($.age >= 28 and $.age < 29)")
            .build();

        Query query = new Query(colorAndAge);
        tryCreateIndex(namespace, INDEXED_SET_NAME, "age_index", "age", IndexType.NUMERIC);
        tryCreateIndex(namespace, INDEXED_SET_NAME, "color_index", "color", IndexType.STRING);
        // An index for a non-existing bin
        tryCreateIndex(namespace, INDEXED_SET_NAME, "some_bin_index", "some_bin", IndexType.STRING);

        // Assert that the query has only filtering Expression and no secondary index Filter due to being an OR query
        assertQueryHasNoSecIndexFilter(query, KeyRecord.class);
        assertThat(query.getCriteriaObject().getFilterExpression()).isEqualTo(Exp.build(
            Exp.or(
                Exp.eq(Exp.bin("color", Exp.Type.STRING), Exp.val(GREEN)),
                Exp.and(
                    Exp.ge(Exp.bin("age", Exp.Type.INT), Exp.val(28)),
                    Exp.lt(Exp.bin("age", Exp.Type.INT), Exp.val(29))
                )
            )
        ));

        try {
            KeyRecordIterator it = queryEngine.select(namespace, INDEXED_SET_NAME, null, query);

            assertThat(it).toIterable().isNotEmpty()
                .allSatisfy(rec -> assertTrue(
                    rec.record.getInt("age") == 28 || rec.record.getString("color").equals(GREEN)));
        } finally {
            tryDropIndex(INDEXED_SET_NAME, "age_index");
            tryDropIndex(INDEXED_SET_NAME, "color_index");
            tryDropIndex(INDEXED_SET_NAME, "some_bin_index");
        }
    }

    @Test
    void selectOnIndexedOrQualifier_withTwoIndexes_usingDslExpression_selectingIndexToUse() {
        // Combining in one DSL expression
        Qualifier colorAndAge = Qualifier.dslExpressionBuilder()
            // Using a static DSL expression with fixed values
            .setDSLExpressionString("$.color == '" + GREEN + "' or ($.age >= 28 and $.age < 29)")
            .setDSLExpressionIndexToUse("color_index")
            .build();

        Query query = new Query(colorAndAge);
        tryCreateIndex(namespace, INDEXED_SET_NAME, "age_index", "age", IndexType.NUMERIC);
        tryCreateIndex(namespace, INDEXED_SET_NAME, "color_index", "color", IndexType.STRING);
        // An index for a non-existing bin
        tryCreateIndex(namespace, INDEXED_SET_NAME, "some_bin_index", "some_bin", IndexType.STRING);

        // Assert that the query has only filtering Expression and no secondary index Filter due to being an OR query
        assertQueryHasNoSecIndexFilter(query, KeyRecord.class);
        assertThat(query.getCriteriaObject().getFilterExpression()).isEqualTo(Exp.build(
            Exp.or(
                Exp.eq(Exp.bin("color", Exp.Type.STRING), Exp.val(GREEN)),
                Exp.and(
                    Exp.ge(Exp.bin("age", Exp.Type.INT), Exp.val(28)),
                    Exp.lt(Exp.bin("age", Exp.Type.INT), Exp.val(29))
                )
            )
        ));

        try {
            KeyRecordIterator it = queryEngine.select(namespace, INDEXED_SET_NAME, null, query);

            assertThat(it).toIterable().isNotEmpty()
                .allSatisfy(rec -> assertTrue(
                    rec.record.getInt("age") == 28 || rec.record.getString("color").equals(GREEN)));
        } finally {
            tryDropIndex(INDEXED_SET_NAME, "age_index");
            tryDropIndex(INDEXED_SET_NAME, "color_index");
            tryDropIndex(INDEXED_SET_NAME, "some_bin_index");
        }
    }
}
