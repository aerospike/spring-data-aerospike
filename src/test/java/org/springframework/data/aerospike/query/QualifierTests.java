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
package org.springframework.data.aerospike.query;

import com.aerospike.client.Value;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.utility.CollectionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.*;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeMetadata.SINCE_UPDATE_TIME;
import static org.springframework.data.aerospike.utility.CollectionUtils.countingInt;

/*
 * Tests to ensure that Qualifiers are built successfully for non indexed bins.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QualifierTests extends BaseQueryEngineTests {

    /*
     * These bins should not be indexed.
     */
    @BeforeAll
    public void dropIndexes() {
        tryDropIndex(SET_NAME, "age_index");
        tryDropIndex(SET_NAME, "color_index");
    }

    @Test
    void throwsExceptionWhenScansDisabled() {
        queryEngine.setScansEnabled(false);
        try {
            Qualifier qualifier = Qualifier.builder()
                .setField("age")
                .setFilterOperation(FilterOperation.LT)
                .setValue1(Value.get(26)).build();

            //noinspection resource
            assertThatThrownBy(() -> queryEngine.select(namespace, SET_NAME, null, new Query(qualifier)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("disabled by default");
        } finally {
            queryEngine.setScansEnabled(true);
        }
    }

    @SuppressWarnings("removal")
    @Test
    void selectOneWitKey() {
        KeyQualifier kq = new KeyQualifier(Value.get("selector-test:3"));

        KeyRecordIterator iterator = queryEngine.select(namespace, SET_NAME, null, new Query(kq));

        assertThat(iterator).toIterable().hasSize(1);
    }

    @SuppressWarnings("removal")
    @Test
    void selectOneWitKeyNonExisting() {
        KeyQualifier kq = new KeyQualifier(Value.get("selector-test:unknown"));

        KeyRecordIterator iterator = queryEngine.select(namespace, SET_NAME, null, new Query(kq));

        assertThat(iterator).toIterable().isEmpty();
    }

    @Test
    void selectAll() {
        KeyRecordIterator iterator = queryEngine.select(namespace, SET_NAME, null, null);

        assertThat(iterator).toIterable().hasSize(RECORD_COUNT);
    }

    @Test
    void lTQualifier() {
        // Ages range from 25 -> 29. We expected to only get back values with age < 26
        Qualifier qualifier = Qualifier.builder()
            .setField("age")
            .setFilterOperation(FilterOperation.LT)
            .setValue1(Value.get(26))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        assertThat(it)
            .toIterable()
            .isNotEmpty()
            .allSatisfy(rec -> assertThat(rec.record.getInt("age")).isLessThan(26))
            .hasSize(queryEngineTestDataPopulator.ageCount.get(25));
    }

    @Test
    void numericLTEQQualifier() {
        // Ages range from 25 -> 29. We expected to only get back values with age <= 26
        Qualifier qualifier = Qualifier.builder()
            .setField("age")
            .setFilterOperation(FilterOperation.LTEQ)
            .setValue1(Value.get(26))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        Map<Integer, Integer> ageCount = CollectionUtils.toStream(it)
            .map(rec -> rec.record.getInt("age"))
            .collect(Collectors.groupingBy(k -> k, countingInt()));
        assertThat(ageCount.keySet())
            .isNotEmpty()
            .allSatisfy(age -> assertThat(age).isLessThanOrEqualTo(26));
        assertThat(ageCount.get(25)).isEqualTo(queryEngineTestDataPopulator.ageCount.get(25));
        assertThat(ageCount.get(26)).isEqualTo(queryEngineTestDataPopulator.ageCount.get(26));
    }

    @Test
    void numericEQQualifier() {
        // Ages range from 25 -> 29. We expected to only get back values with age == 26
        Qualifier qualifier = Qualifier.builder()
            .setField("age")
            .setFilterOperation(FilterOperation.EQ)
            .setValue1(Value.get(26))
            .build();

        KeyRecordIterator iterator = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        assertThat(iterator)
            .toIterable()
            .isNotEmpty()
            .allSatisfy(rec -> assertThat(rec.record.getInt("age")).isEqualTo(26))
            .hasSize(queryEngineTestDataPopulator.ageCount.get(26));
    }

    @Test
    void numericGTEQQualifier() {
        // Ages range from 25 -> 29. We expected to only get back values with age >= 28
        Qualifier qualifier = Qualifier.builder()
            .setField("age")
            .setFilterOperation(FilterOperation.GTEQ)
            .setValue1(Value.get(28))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        Map<Integer, Integer> ageCount = CollectionUtils.toStream(it)
            .map(rec -> rec.record.getInt("age"))
            .collect(Collectors.groupingBy(k -> k, countingInt()));
        assertThat(ageCount.keySet())
            .isNotEmpty()
            .allSatisfy(age -> assertThat(age).isGreaterThanOrEqualTo(28));
        assertThat(ageCount.get(28)).isEqualTo(queryEngineTestDataPopulator.ageCount.get(28));
        assertThat(ageCount.get(29)).isEqualTo(queryEngineTestDataPopulator.ageCount.get(29));
    }

    @Test
    void numericGTQualifier() {
        // Ages range from 25 -> 29. We expected to only get back values with age > 28 or equivalently == 29
        Qualifier qualifier = Qualifier.builder()
            .setField("age")
            .setFilterOperation(FilterOperation.GT)
            .setValue1(Value.get(28))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        assertThat(it)
            .toIterable()
            .isNotEmpty()
            .allSatisfy(rec -> assertThat(rec.record.getInt("age")).isEqualTo(29))
            .hasSize(queryEngineTestDataPopulator.ageCount.get(29));
    }

    @Test
    void metadataSinceUpdateEQQualifier() {
        Qualifier qualifier = Qualifier.metadataBuilder()
            .setMetadataField(SINCE_UPDATE_TIME)
            .setFilterOperation(FilterOperation.GT)
            .setValue1AsObj(1L)
            .build();

        KeyRecordIterator iterator = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        assertThat(iterator)
            .toIterable()
            .isNotEmpty()
            .allSatisfy(item -> assertThat(item.record.getInt("age")).isPositive())
            .hasSize(RECORD_COUNT);
    }

    @Test
    void stringEQQualifier() {
        Qualifier qualifier = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.EQ)
            .setValue1(Value.get(ORANGE))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        assertThat(it)
            .toIterable()
            .isNotEmpty()
            .allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(ORANGE))
            .hasSize(queryEngineTestDataPopulator.colourCounts.get(ORANGE));
    }

    @Test
    void stringEQIgnoreCaseQualifier() {
        Qualifier qualifier = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.EQ)
            .setIgnoreCase(true)
            .setValue1(Value.get(ORANGE.toUpperCase()))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        assertThat(it)
            .toIterable()
            .isNotEmpty()
            .allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(ORANGE))
            .hasSize(queryEngineTestDataPopulator.colourCounts.get(ORANGE));
    }

    @Test
    void stringEqualIgnoreCaseWorksOnUnindexedBin() {
        boolean ignoreCase = true;
        Qualifier qualifier = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.EQ)
            .setIgnoreCase(ignoreCase)
            .setValue1(Value.get("BlUe"))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        assertThat(it)
            .toIterable()
            .isNotEmpty()
            .allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(BLUE))
            .hasSize(queryEngineTestDataPopulator.colourCounts.get(BLUE));
    }

    @Test
    void stringEqualIgnoreCaseWorksOnIndexedBin() {
        withIndex(namespace, SET_NAME, "color_index_selector", "color", IndexType.STRING, () -> {
            boolean ignoreCase = true;
            Qualifier qualifier = Qualifier.builder()
                .setField("color")
                .setFilterOperation(FilterOperation.EQ)
                .setIgnoreCase(ignoreCase)
                .setValue1(Value.get("BlUe"))
                .build();

            KeyRecordIterator iterator = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

            assertThat(iterator)
                .toIterable()
                .isNotEmpty()
                .allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(BLUE))
                .hasSize(queryEngineTestDataPopulator.colourCounts.get(BLUE));
            // scan will be run, since Aerospike filter does not support case-insensitive string comparison
        });

        tryDropIndex(SET_NAME, "color_index");
    }

    @Test
    void stringEqualIgnoreCaseWorksRequiresFullMatch() {
        boolean ignoreCase = true;
        Qualifier qualifier = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.EQ)
            .setIgnoreCase(ignoreCase)
            .setValue1(Value.get("lue"))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        assertThat(it).toIterable().isEmpty();
    }

    @Test
    void stringStartWithQualifier() {
        Qualifier qualifier = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.STARTS_WITH)
            .setValue1(Value.get(BLUE.substring(0, 2)))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        assertThat(it)
            .toIterable()
            .isNotEmpty()
            .allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(BLUE))
            .hasSize(queryEngineTestDataPopulator.colourCounts.get(BLUE));
    }

    @Test
    void stringStartWithEntireWordQualifier() {
        Qualifier qualifier = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.STARTS_WITH)
            .setValue1(Value.get(BLUE))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        assertThat(it)
            .toIterable()
            .isNotEmpty()
            .allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(BLUE))
            .hasSize(queryEngineTestDataPopulator.colourCounts.get(BLUE));
    }

    @Test
    void stringStartWithICASEQualifier() {
        Qualifier qualifier = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.STARTS_WITH)
            .setIgnoreCase(true)
            .setValue1(Value.get("BLU"))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        assertThat(it)
            .toIterable()
            .isNotEmpty()
            .allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(BLUE))
            .hasSize(queryEngineTestDataPopulator.colourCounts.get(BLUE));
    }

    @Test
    void stringEndsWithQualifier() {
        Qualifier qualifier = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.ENDS_WITH)
            .setValue1(Value.get(GREEN.substring(2)))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        assertThat(it)
            .toIterable()
            .isNotEmpty()
            .allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(GREEN))
            .hasSize(queryEngineTestDataPopulator.colourCounts.get(GREEN));
    }

    @Test
    void selectEndsWith() {
        Qualifier qualifier = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.ENDS_WITH)
            .setValue1(Value.get("e"))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        assertThat(it)
            .toIterable()
            .isNotEmpty()
            .allSatisfy(rec -> assertThat(rec.record.getString("color")).isIn(BLUE, ORANGE))
            .hasSize(queryEngineTestDataPopulator.colourCounts.get(BLUE) + queryEngineTestDataPopulator.colourCounts.get(ORANGE));
    }

    @Test
    void stringEndsWithEntireWordQualifier() {
        Qualifier qualifier = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.ENDS_WITH)
            .setValue1(Value.get(GREEN))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        assertThat(it)
            .toIterable()
            .isNotEmpty()
            .allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(GREEN))
            .hasSize(queryEngineTestDataPopulator.colourCounts.get(GREEN));
    }

    @Test
    void betweenQualifier() {
        // Ages range from 25 -> 29. Get back age between 26 and 28 inclusive
        Qualifier qualifier = Qualifier.builder()
            .setField("age")
            .setFilterOperation(FilterOperation.BETWEEN)
            .setValue1(Value.get(26))
            .setValue2(Value.get(29)) // + 1 as upper limit is exclusive
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        Map<Integer, Integer> ageCount = CollectionUtils.toStream(it)
            .map(rec -> rec.record.getInt("age"))
            .collect(Collectors.groupingBy(k -> k, countingInt()));
        assertThat(ageCount.keySet())
            .isNotEmpty()
            .allSatisfy(age -> assertThat(age).isBetween(26, 28));
        assertThat(ageCount.get(26)).isEqualTo(queryEngineTestDataPopulator.ageCount.get(26));
        assertThat(ageCount.get(27)).isEqualTo(queryEngineTestDataPopulator.ageCount.get(27));
        assertThat(ageCount.get(28)).isEqualTo(queryEngineTestDataPopulator.ageCount.get(28));
    }

    @Test
    void containingQualifier() {
        Map<String, Integer> expectedCounts = Arrays.stream(COLOURS)
            .filter(c -> c.contains("l"))
            .collect(Collectors.toMap(color -> color, color -> queryEngineTestDataPopulator.colourCounts.get(color)));

        Qualifier qualifier = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.CONTAINING)
            .setValue1(Value.get("l"))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        Map<String, Integer> colorCount = CollectionUtils.toStream(it)
            .map(rec -> rec.record.getString("color"))
            .collect(Collectors.groupingBy(k -> k, countingInt()));
        assertThat(colorCount).isNotEmpty().isEqualTo(expectedCounts);
    }

    @Test
    void inQualifier() {
        List<String> inColors = Arrays.asList(COLOURS[0], COLOURS[2]);
        Map<String, Integer> expectedCounts = inColors.stream()
            .collect(Collectors.toMap(color -> color, color -> queryEngineTestDataPopulator.colourCounts.get(color)));

        Qualifier qualifier = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.IN)
            .setValue1(Value.get(inColors))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        Map<String, Integer> colorCount = CollectionUtils.toStream(it)
            .map(rec -> rec.record.getString("color"))
            .collect(Collectors.groupingBy(k -> k, countingInt()));
        assertThat(colorCount).isNotEmpty().isEqualTo(expectedCounts);
    }

    @Test
    void listContainsQualifier() {
        String searchColor = COLOURS[0];
        String binName = "colorList";

        Qualifier qualifier = Qualifier.builder()
            .setField(binName)
            .setFilterOperation(FilterOperation.LIST_VAL_CONTAINING)
            .setValue1(Value.get(searchColor))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        assertThat(it).toIterable()
            .isNotEmpty()
            .allSatisfy(rec -> {
                @SuppressWarnings("unchecked")
                List<String> colorList = (List<String>) rec.record.getList(binName);
                String color = colorList.get(0);
                assertThat(color).isEqualTo(searchColor);
            })
            .hasSize(queryEngineTestDataPopulator.colourCounts.get(searchColor));
    }

    @Test
    void listBetweenQualifier() {
        long ageStart = AGES[0]; // 25
        long ageEnd = AGES[2]; // 27
        String binName = "longList";

        Qualifier qualifier = Qualifier.builder()
            .setField(binName)
            .setFilterOperation(FilterOperation.LIST_VAL_BETWEEN)
            .setValue1(Value.get(ageStart))
            .setValue2(Value.get(ageEnd + 1L))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        Map<Long, Integer> ageCount = CollectionUtils.toStream(it)
            .map(rec -> {
                @SuppressWarnings("unchecked") List<Long> ageList = (List<Long>) rec.record.getList(binName);
                return ageList.get(0);
            })
            .collect(Collectors.groupingBy(k -> k, countingInt()));
        assertThat(ageCount.keySet())
            .isNotEmpty()
            .allSatisfy(age -> assertThat(age).isBetween(ageStart, ageEnd));
        assertThat(ageCount.get(25L)).isEqualTo(queryEngineTestDataPopulator.ageCount.get(25));
        assertThat(ageCount.get(26L)).isEqualTo(queryEngineTestDataPopulator.ageCount.get(26));
        assertThat(ageCount.get(27L)).isEqualTo(queryEngineTestDataPopulator.ageCount.get(27));
    }

    @Test
    void mapKeysContainsQualifier() {
        String searchColor = COLOURS[0];
        String binName = "colorAgeMap";

        Qualifier qualifier = Qualifier.builder()
            .setField(binName)
            .setFilterOperation(FilterOperation.MAP_KEYS_CONTAIN)
            .setValue1(Value.get(searchColor))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        assertThat(it).toIterable()
            .isNotEmpty()
            .allSatisfy(rec -> {
                @SuppressWarnings("unchecked")
                Map<String, ?> colorMap = (Map<String, ?>) rec.record.getMap(binName);
                assertThat(colorMap).containsKey(searchColor);
            })
            .hasSize(queryEngineTestDataPopulator.colourCounts.get(searchColor));
    }

    @Test
    void mapValuesContainsQualifier() {
        String searchColor = COLOURS[0];
        String binName = "ageColorMap";

        Qualifier qualifier = Qualifier.builder()
            .setField(binName)
            .setFilterOperation(FilterOperation.MAP_VALUES_CONTAIN)
            .setValue1(Value.get(searchColor))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        assertThat(it).toIterable()
            .isNotEmpty()
            .allSatisfy(rec -> {
                @SuppressWarnings("unchecked")
                Map<?, String> colorMap = (Map<?, String>) rec.record.getMap(binName);
                assertThat(colorMap).containsValue(searchColor);
            })
            .hasSize(queryEngineTestDataPopulator.colourCounts.get(searchColor));
    }

    @Test
    void mapKeysBetweenQualifier() {
        long ageStart = AGES[0]; // 25
        long ageEnd = AGES[2]; // 27
        String binName = "ageColorMap";

        Qualifier qualifier = Qualifier.builder()
            .setField(binName)
            .setFilterOperation(FilterOperation.MAP_KEYS_BETWEEN)
            .setValue1(Value.get(ageStart))
            .setValue2(Value.get(ageEnd + 1L))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        Map<Long, Integer> ageCount = CollectionUtils.toStream(it)
            .map(rec -> {
                @SuppressWarnings("unchecked")
                Map<Long, ?> ageColorMap = (Map<Long, ?>) rec.record.getMap(binName);
                // This is always a one item map
                //noinspection OptionalGetWithoutIsPresent
                return ageColorMap.keySet().stream().filter(val -> val != SKIP_LONG_VALUE).findFirst().get();
            })
            .collect(Collectors.groupingBy(k -> k, countingInt()));
        assertThat(ageCount.keySet())
            .isNotEmpty()
            .allSatisfy(age -> assertThat(age).isBetween(ageStart, ageEnd));
        assertThat(ageCount.get(25L)).isEqualTo(queryEngineTestDataPopulator.ageCount.get(25));
        assertThat(ageCount.get(26L)).isEqualTo(queryEngineTestDataPopulator.ageCount.get(26));
        assertThat(ageCount.get(27L)).isEqualTo(queryEngineTestDataPopulator.ageCount.get(27));
    }

    @Test
    void mapValuesBetweenQualifier() {
        long ageStart = AGES[0]; // 25
        long ageEnd = AGES[2]; // 27
        String binName = "colorAgeMap";

        Qualifier qualifier = Qualifier.builder()
            .setField(binName)
            .setFilterOperation(FilterOperation.MAP_VAL_BETWEEN)
            .setValue1(Value.get(ageStart))
            .setValue2(Value.get(ageEnd + 1L))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        Map<Long, Integer> ageCount = CollectionUtils.toStream(it)
            .map(rec -> {
                @SuppressWarnings("unchecked")
                Map<?, Long> ageColorMap = (Map<?, Long>) rec.record.getMap(binName);
                // This is always a one item map
                //noinspection OptionalGetWithoutIsPresent
                return ageColorMap.values().stream().filter(val -> val != SKIP_LONG_VALUE).findFirst().get();
            })
            .collect(Collectors.groupingBy(k -> k, countingInt()));
        assertThat(ageCount.keySet())
            .isNotEmpty()
            .allSatisfy(age -> assertThat(age).isBetween(ageStart, ageEnd));
        assertThat(ageCount.get(25L)).isEqualTo(queryEngineTestDataPopulator.ageCount.get(25));
        assertThat(ageCount.get(26L)).isEqualTo(queryEngineTestDataPopulator.ageCount.get(26));
        assertThat(ageCount.get(27L)).isEqualTo(queryEngineTestDataPopulator.ageCount.get(27));
    }

    @Test
    void containingDoesNotUseSpecialCharacterQualifier() {
        Qualifier qualifier = Qualifier.builder()
            .setField(SPECIAL_CHAR_BIN)
            .setFilterOperation(FilterOperation.CONTAINING)
            .setValue1(Value.get(".*"))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SPECIAL_CHAR_SET, null, new Query(qualifier));

        assertThat(it).toIterable()
            .isNotEmpty()
            .allSatisfy(rec -> assertThat(rec.record.getString(SPECIAL_CHAR_BIN)).contains(".*"))
            .hasSize(3);
    }

    @Test
    void startWithDoesNotUseSpecialCharacterQualifier() {
        Qualifier qualifier = Qualifier.builder()
            .setField(SPECIAL_CHAR_BIN)
            .setFilterOperation(FilterOperation.STARTS_WITH)
            .setValue1(Value.get(".*"))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SPECIAL_CHAR_SET, null, new Query(qualifier));

        assertThat(it).toIterable()
            .isNotEmpty()
            .allSatisfy(rec -> assertThat(rec.record.getString(SPECIAL_CHAR_BIN)).startsWith(".*"))
            .hasSize(1);
    }

    @Test
    void endWithDoesNotUseSpecialCharacterQualifier() {
        Qualifier qualifier = Qualifier.builder()
            .setField(SPECIAL_CHAR_BIN)
            .setFilterOperation(FilterOperation.ENDS_WITH)
            .setValue1(Value.get(".*"))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SPECIAL_CHAR_SET, null, new Query(qualifier));

        assertThat(it).toIterable()
            .isNotEmpty()
            .allSatisfy(rec -> assertThat(rec.record.getString(SPECIAL_CHAR_BIN)).endsWith(".*"))
            .hasSize(1);
    }

    @Test
    void eQIcaseDoesNotUseSpecialCharacter() {
        Qualifier qualifier = Qualifier.builder()
            .setField(SPECIAL_CHAR_BIN)
            .setFilterOperation(FilterOperation.EQ)
            .setIgnoreCase(true)
            .setValue1(Value.get(".*"))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SPECIAL_CHAR_SET, null, new Query(qualifier));

        assertThat(it).toIterable().isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"[", "$", "\\", "^"})
    void containingFindsSquareBracket(String specialString) {
        Qualifier qualifier = Qualifier.builder()
            .setField(SPECIAL_CHAR_BIN)
            .setFilterOperation(FilterOperation.CONTAINING)
            .setIgnoreCase(true)
            .setValue1(Value.get(specialString))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SPECIAL_CHAR_SET, null, new Query(qualifier));

        assertThat(it).toIterable()
            .isNotEmpty()
            .allSatisfy(rec -> assertThat(rec.record.getString(SPECIAL_CHAR_BIN)).contains(specialString))
            .hasSize(1);
    }

    @Test
    void selectWithGeoWithin() {
        double lon = -122.0;
        double lat = 37.5;
        double radius = 50000.0;
        String rgnstr = String.format("{ \"type\": \"AeroCircle\", "
                + "\"coordinates\": [[%.8f, %.8f], %f] }",
            lon, lat, radius);
        Qualifier qualifier = Qualifier.builder()
            .setField(GEO_BIN_NAME)
            .setFilterOperation(FilterOperation.GEO_WITHIN)
            .setValue1(Value.getAsGeoJSON(rgnstr))
            .build();

        KeyRecordIterator iterator = queryEngine.select(namespace, GEO_SET, null, new Query(qualifier));

        assertThat(iterator).toIterable()
            .isNotEmpty()
            .allSatisfy(rec -> assertThat(rec.record.generation).isGreaterThanOrEqualTo(1));
    }

    @Test
    void startWithAndEqualIgnoreCaseReturnsAllItems() {
        boolean ignoreCase = true;
        Qualifier qual1 = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.EQ)
            .setIgnoreCase(ignoreCase)
            .setValue1(Value.get(BLUE.toUpperCase()))
            .build();

        Qualifier qual2 = Qualifier.builder()
            .setField("name")
            .setFilterOperation(FilterOperation.STARTS_WITH)
            .setIgnoreCase(ignoreCase)
            .setValue1(Value.get("NA"))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(Qualifier.and(qual1, qual2)));

        assertThat(it).toIterable()
            .isNotEmpty()
            .allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(BLUE))
            .hasSize(queryEngineTestDataPopulator.colourCounts.get(BLUE));
    }

    @Test
    void equalIgnoreCaseReturnsNoItemsIfNoneMatched() {
        boolean ignoreCase = false;
        Qualifier qual1 = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.EQ)
            .setIgnoreCase(ignoreCase)
            .setValue1(Value.get(BLUE.toUpperCase()))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qual1));

        assertThat(it).toIterable().isEmpty();
    }

    @Test
    void startWithIgnoreCaseReturnsNoItemsIfNoneMatched() {
        boolean ignoreCase = false;
        Qualifier qual1 = Qualifier.builder()
            .setField("name")
            .setFilterOperation(FilterOperation.STARTS_WITH)
            .setIgnoreCase(ignoreCase)
            .setValue1(Value.get("NA"))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qual1));

        assertThat(it).toIterable().isEmpty();
    }

    @Test
    void selectWithBetweenAndOrQualifiers() {
        Qualifier colorIsGreen = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.EQ)
            .setValue1(Value.get(GREEN))
            .build();
        Qualifier ageBetween28And29 = Qualifier.builder()
            .setField("age")
            .setFilterOperation(FilterOperation.BETWEEN)
            .setValue1(Value.get(28))
            .setValue2(Value.get(29))
            .build();
        Qualifier ageIs25 = Qualifier.builder()
            .setField("age")
            .setFilterOperation(FilterOperation.EQ)
            .setValue1(Value.get(25))
            .build();
        Qualifier nameIs696 = Qualifier.builder()
            .setField("name")
            .setFilterOperation(FilterOperation.EQ)
            .setValue1(Value.get("name:696"))
            .build();

        Qualifier or = Qualifier.or(ageIs25, ageBetween28And29, nameIs696);
        Qualifier or2 = Qualifier.or(colorIsGreen, nameIs696);
        Qualifier qualifier = Qualifier.and(or, or2);

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));

        assertThat(it).toIterable().isNotEmpty()
            .allSatisfy(rec -> {
                int age = rec.record.getInt("age");
                String color = rec.record.getString("color");
                String name = rec.record.getString("name");

                assertThat(rec).satisfiesAnyOf(
                    r -> assertThat(age).isEqualTo(25),
                    r -> assertThat(age).isBetween(28, 29),
                    r -> assertThat(name).isEqualTo("name:696")
                );
                assertThat(rec).satisfiesAnyOf(
                    r -> assertThat(color).isEqualTo(GREEN),
                    r -> assertThat(name).isEqualTo("name:696")
                );
            });
    }

    @Test
    void selectWithOrQualifiers() {
        // We are expecting to get back all records where color == blue or (age == 28 || age == 29)
        Qualifier colorIsBlue = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.EQ)
            .setValue1(Value.get(BLUE))
            .build();
        Qualifier ageBetween28And29 = Qualifier.builder()
            .setField("age")
            .setFilterOperation(FilterOperation.BETWEEN)
            .setValue1(Value.get(28))
            .setValue2(Value.get(30)) // + 1 as upper limit is exclusive
            .build();

        Qualifier or = Qualifier.or(colorIsBlue, ageBetween28And29);

        KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, new Query(or));

        List<KeyRecord> result = CollectionUtils.toStream(it).collect(Collectors.toList());
        assertThat(result)
            .isNotEmpty()
            .allSatisfy(rec -> {
                int age = rec.record.getInt("age");
                String color = rec.record.getString("color");

                assertThat(rec).satisfiesAnyOf(
                    r -> assertThat(color).isEqualTo(BLUE),
                    r -> assertThat(age).isBetween(28, 29)
                );
            });
        assertThat(result.stream().map(rec -> rec.record.getInt("age")))
            .filteredOn(age -> age >= 28 && age <= 29)
            .isNotEmpty()
            .hasSize(queryEngineTestDataPopulator.ageCount.get(28) + queryEngineTestDataPopulator.ageCount.get(29));
        assertThat(result.stream().map(rec -> rec.record.getString("color")))
            .filteredOn(color -> color.equals(BLUE))
            .isNotEmpty()
            .hasSize(queryEngineTestDataPopulator.colourCounts.get(BLUE));
    }
}
