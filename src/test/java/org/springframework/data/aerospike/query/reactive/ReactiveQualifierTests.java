/*
 * Copyright 2012-2019 Aerospike, Inc.
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
package org.springframework.data.aerospike.query.reactive;

import com.aerospike.client.Value;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.BLUE;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.COLOURS;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.GREEN;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.ORANGE;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.SET_NAME;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.SPECIAL_CHAR_BIN;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.SPECIAL_CHAR_SET;
import static org.springframework.data.aerospike.utility.CollectionUtils.countingInt;

/*
 * Tests to ensure that Qualifiers are built successfully for non indexed bins.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReactiveQualifierTests extends BaseReactiveQueryEngineTests {

    /*
     * These bins should not be indexed.
     */
    @BeforeAll
    public void dropIndexes() {
        super.tryDropIndex(namespace, SET_NAME, "age_index");
        super.tryDropIndex(namespace, SET_NAME, "color_index");
    }

    @Test
    void throwsExceptionWhenScansDisabled() {
        queryEngine.setScansEnabled(false);
        try {
            Qualifier qualifier = Qualifier.builder()
                .setField("age")
                .setFilterOperation(FilterOperation.LT)
                .setValue(Value.get(26))
                .build();

            StepVerifier.create(queryEngine.select(namespace, SET_NAME, null, new Query(qualifier)))
                .expectErrorSatisfies(e -> assertThat(e)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("disabled by default"))
                .verify();
        } finally {
            queryEngine.setScansEnabled(true);
        }
    }

    @Test
    public void lTQualifier() {
        // Ages range from 25 -> 29. We expected to only get back values with age < 26
        Qualifier ageRangeQualifier = Qualifier.builder()
            .setField("age")
            .setFilterOperation(FilterOperation.LT)
            .setValue(Value.get(26))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, new Query(ageRangeQualifier));
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                assertThat(results)
                    .filteredOn(keyRecord -> {
                        int age = keyRecord.record.getInt("age");
                        assertThat(age).isLessThan(26);
                        return age == 25;
                    })
                    .hasSize(queryEngineTestDataPopulator.ageCount.get(25));
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void numericLTEQQualifier() {
        // Ages range from 25 -> 29. We expected to only get back values with age <= 26
        Qualifier ageRangeQualifier = Qualifier.builder()
            .setField("age")
            .setFilterOperation(FilterOperation.LTEQ)
            .setValue(Value.get(26))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, new Query(ageRangeQualifier));
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                AtomicInteger age25Count = new AtomicInteger();
                AtomicInteger age26Count = new AtomicInteger();
                results.forEach(keyRecord -> {
                    int age = keyRecord.record.getInt("age");
                    assertThat(age).isLessThanOrEqualTo(26);

                    if (age == 25) {
                        age25Count.incrementAndGet();
                    } else if (age == 26) {
                        age26Count.incrementAndGet();
                    }
                });
                assertThat(age25Count.get()).isEqualTo(queryEngineTestDataPopulator.ageCount.get(25));
                assertThat(age26Count.get()).isEqualTo(queryEngineTestDataPopulator.ageCount.get(26));
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void numericEQQualifier() {
        // Ages range from 25 -> 29. We expected to only get back values with age == 26
        Qualifier ageRangeQualifier = Qualifier.builder()
            .setField("age")
            .setFilterOperation(FilterOperation.EQ)
            .setValue(Value.get(26))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, new Query(ageRangeQualifier));

        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                assertThat(results)
                    .allSatisfy(rec -> assertThat(rec.record.getInt("age")).isEqualTo(26))
                    .hasSize(queryEngineTestDataPopulator.ageCount.get(26));
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void numericGTEQQualifier() {
        // Ages range from 25 -> 29. We expected to only get back values with age >= 28
        Qualifier ageRangeQualifier = Qualifier.builder()
            .setField("age")
            .setFilterOperation(FilterOperation.GTEQ)
            .setValue(Value.get(28))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, new Query(ageRangeQualifier));
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                AtomicInteger age28Count = new AtomicInteger();
                AtomicInteger age29Count = new AtomicInteger();
                results.forEach(keyRecord -> {
                    int age = keyRecord.record.getInt("age");
                    assertThat(age).isGreaterThanOrEqualTo(28);

                    if (age == 28) {
                        age28Count.incrementAndGet();
                    } else if (age == 29) {
                        age29Count.incrementAndGet();
                    }
                });
                assertThat(age28Count.get()).isEqualTo(queryEngineTestDataPopulator.ageCount.get(25));
                assertThat(age29Count.get()).isEqualTo(queryEngineTestDataPopulator.ageCount.get(26));
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void numericGTQualifier() {
        // Ages range from 25 -> 29. We expected to only get back values with age > 28 or equivalently == 29
        Qualifier ageRangeQualifier = Qualifier.builder()
            .setField("age")
            .setFilterOperation(FilterOperation.GT)
            .setValue(Value.get(28))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, new Query(ageRangeQualifier));
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                assertThat(results)
                    .allSatisfy(rec -> assertThat(rec.record.getInt("age")).isEqualTo(29))
                    .hasSize(queryEngineTestDataPopulator.ageCount.get(29));
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void stringEQQualifier() {
        Qualifier stringEqQualifier = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.EQ)
            .setValue(Value.get(ORANGE))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, new Query(stringEqQualifier));
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                assertThat(results)
                    .allSatisfy(rec -> assertThat(rec.record.getString("color")).endsWith(ORANGE))
                    .hasSize(queryEngineTestDataPopulator.colourCounts.get(ORANGE));
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void stringEQQualifierCaseSensitive() {
        Qualifier stringEqQualifier = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.EQ)
            .setIgnoreCase(true)
            .setValue(Value.get(ORANGE.toUpperCase()))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, new Query(stringEqQualifier));
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                assertThat(results)
                    .allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(ORANGE))
                    .hasSize(queryEngineTestDataPopulator.colourCounts.get(ORANGE));
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void stringStartWithQualifier() {
        String bluePrefix = "blu";

        Qualifier stringEqQualifier = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.STARTS_WITH)
            .setValue(Value.get("blu"))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, new Query(stringEqQualifier));
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                assertThat(results)
                    .allSatisfy(rec -> assertThat(rec.record.getString("color")).startsWith(bluePrefix))
                    .hasSize(queryEngineTestDataPopulator.colourCounts.get(BLUE));
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void stringStartWithEntireWordQualifier() {
        Qualifier stringEqQualifier = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.STARTS_WITH)
            .setValue(Value.get(BLUE))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, new Query(stringEqQualifier));
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                assertThat(results)
                    .allSatisfy(rec -> assertThat(rec.record.getString("color")).startsWith(BLUE))
                    .hasSize(queryEngineTestDataPopulator.colourCounts.get(BLUE));
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void stringStartWithICASEQualifier() {
        String blue = "blu";

        Qualifier stringEqQualifier = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.STARTS_WITH)
            .setIgnoreCase(true)
            .setValue(Value.get("BLU"))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, new Query(stringEqQualifier));
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                assertThat(results)
                    .allSatisfy(rec -> assertThat(rec.record.getString("color")).startsWith(blue))
                    .hasSize(queryEngineTestDataPopulator.colourCounts.get(BLUE));
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void stringEndsWithQualifier() {
        String greenEnding = GREEN.substring(2);

        Qualifier stringEqQualifier = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.ENDS_WITH)
            .setValue(Value.get(greenEnding))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, new Query(stringEqQualifier));
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                assertThat(results)
                    .allSatisfy(rec -> assertThat(rec.record.getString("color")).endsWith(greenEnding))
                    .hasSize(queryEngineTestDataPopulator.colourCounts.get(GREEN));
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void stringEndsWithEntireWordQualifier() {
        Qualifier stringEqQualifier = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.ENDS_WITH)
            .setValue(Value.get(GREEN))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, new Query(stringEqQualifier));
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                assertThat(results)
                    .allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(GREEN))
                    .hasSize(queryEngineTestDataPopulator.colourCounts.get(GREEN));
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void betweenQualifier() {
        // Ages range from 25 -> 29. Get back age between 26 and 28 inclusive
        Qualifier ageRangeQualifier = Qualifier.builder()
            .setField("age")
            .setFilterOperation(FilterOperation.BETWEEN)
            .setValue(Value.get(26))
            .setSecondValue(Value.get(29)) // + 1 as upper limit is exclusive
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, new Query(ageRangeQualifier));
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                AtomicInteger age26Count = new AtomicInteger();
                AtomicInteger age27Count = new AtomicInteger();
                AtomicInteger age28Count = new AtomicInteger();
                results.forEach(keyRecord -> {
                    int age = keyRecord.record.getInt("age");
                    assertThat(age).isBetween(26, 28);
                    if (age == 26) {
                        age26Count.incrementAndGet();
                    } else if (age == 27) {
                        age27Count.incrementAndGet();
                    } else {
                        age28Count.incrementAndGet();
                    }
                });
                assertThat(age26Count.get()).isEqualTo(queryEngineTestDataPopulator.ageCount.get(26));
                assertThat(age27Count.get()).isEqualTo(queryEngineTestDataPopulator.ageCount.get(27));
                assertThat(age28Count.get()).isEqualTo(queryEngineTestDataPopulator.ageCount.get(28));
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void containingQualifier() {
        Map<String, Integer> expectedColorCounts = Arrays.stream(COLOURS)
            .filter(c -> c.contains("l"))
            .collect(Collectors.toMap(c -> c, color -> queryEngineTestDataPopulator.colourCounts.get(color)));

        Qualifier ageRangeQualifier = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.CONTAINING)
            .setValue(Value.get("l"))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, new Query(ageRangeQualifier));
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                Map<String, Integer> actualColors = results.stream()
                    .map(rec -> rec.record.getString("color"))
                    .collect(Collectors.groupingBy(k -> k, countingInt()));
                assertThat(actualColors).isEqualTo(expectedColorCounts);
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void inQualifier() {
        List<String> inColours = Arrays.asList(COLOURS[0], COLOURS[2]);
        Map<String, Integer> expectedColorCounts = inColours.stream()
            .collect(Collectors.toMap(c -> c, color -> queryEngineTestDataPopulator.colourCounts.get(color)));

        Qualifier qualifier = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.IN)
            .setValue(Value.get(inColours))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, new Query(qualifier));
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                Map<String, Integer> actualColors = results.stream()
                    .map(rec -> rec.record.getString("color"))
                    .collect(Collectors.groupingBy(k -> k, countingInt()));
                assertThat(actualColors).isEqualTo(expectedColorCounts);
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void listContainsQualifier() {
        String searchColor = COLOURS[0];

        String binName = "colorList";

        Qualifier ageRangeQualifier = Qualifier.builder()
            .setField(binName)
            .setFilterOperation(FilterOperation.COLLECTION_VAL_CONTAINING)
            .setValue(Value.get(searchColor))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, new Query(ageRangeQualifier));
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                // Every Record with a color == "color" has a one element list ["color"]
                // so there are an equal amount of records with the list == [lcolor"] as with a color == "color"
                assertThat(results)
                    .allSatisfy(rec -> {
                        @SuppressWarnings("unchecked") List<String> colorList =
                            (List<String>) rec.record.getList(binName);
                        String color = colorList.get(0);
                        assertThat(color).isEqualTo(searchColor);
                    })
                    .hasSize(queryEngineTestDataPopulator.colourCounts.get(searchColor));
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void mapKeysContainQualifier() {
        String searchColor = COLOURS[0];

        String binName = "colorAgeMap";

        Qualifier ageRangeQualifier = Qualifier.builder()
            .setField(binName)
            .setFilterOperation(FilterOperation.MAP_KEYS_CONTAIN)
            .setValue(Value.get(searchColor))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, new Query(ageRangeQualifier));
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                // Every Record with a color == "color" has a one element map {"color" => #}
                // so there are an equal amount of records with the map {"color" => #} as with a color == "color"
                assertThat(results)
                    .allSatisfy(rec -> {
                        @SuppressWarnings("unchecked") Map<String, ?> colorMap =
                            (Map<String, ?>) rec.record.getMap(binName);
                        assertThat(colorMap).containsKey(searchColor);
                    })
                    .hasSize(queryEngineTestDataPopulator.colourCounts.get(searchColor));
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void testMapValuesContainQualifier() {
        String searchColor = COLOURS[0];

        String binName = "ageColorMap";

        Qualifier ageRangeQualifier = Qualifier.builder()
            .setField(binName)
            .setFilterOperation(FilterOperation.MAP_VALUES_CONTAIN)
            .setValue(Value.get(searchColor))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, new Query(ageRangeQualifier));
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                // Every Record with a color == "color" has a one element map {"color" => #}
                // so there are an equal amount of records with the map {"color" => #} as with a color == "color"
                assertThat(results)
                    .allSatisfy(rec -> {
                        @SuppressWarnings("unchecked") Map<?, String> colorMap =
                            (Map<?, String>) rec.record.getMap(binName);
                        assertThat(colorMap).containsValue(searchColor);
                    })
                    .hasSize(queryEngineTestDataPopulator.colourCounts.get(searchColor));
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void testContainingDoesNotUseSpecialCharacterQualifier() {
        Qualifier ageRangeQualifier = Qualifier.builder()
            .setField(SPECIAL_CHAR_BIN)
            .setFilterOperation(FilterOperation.CONTAINING)
            .setValue(Value.get(".*"))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SPECIAL_CHAR_SET, null, new Query(ageRangeQualifier));
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                assertThat(results)
                    .allSatisfy(rec -> assertThat(rec.record.getString(SPECIAL_CHAR_BIN)).contains(".*"))
                    .hasSize(3);
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void testStartWithDoesNotUseSpecialCharacterQualifier() {
        Qualifier ageRangeQualifier = Qualifier.builder()
            .setField(SPECIAL_CHAR_BIN)
            .setFilterOperation(FilterOperation.STARTS_WITH)
            .setValue(Value.get(".*"))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SPECIAL_CHAR_SET, null, new Query(ageRangeQualifier));
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                assertThat(results)
                    .allSatisfy(rec -> {
                        String scBin = rec.record.getString(SPECIAL_CHAR_BIN);
                        assertThat(scBin).startsWith(".*");
                    })
                    .hasSize(1);
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void testEndWithDoesNotUseSpecialCharacterQualifier() {
        Qualifier ageRangeQualifier = Qualifier.builder()
            .setField(SPECIAL_CHAR_BIN)
            .setFilterOperation(FilterOperation.ENDS_WITH)
            .setValue(Value.get(".*"))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SPECIAL_CHAR_SET, null, new Query(ageRangeQualifier));
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                assertThat(results)
                    .allSatisfy(rec -> {
                        String scBin = rec.record.getString(SPECIAL_CHAR_BIN);
                        assertThat(scBin).endsWith(".*");
                    })
                    .hasSize(1);
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void testEQICaseDoesNotUseSpecialCharacter() {
        Qualifier ageRangeQualifier = Qualifier.builder()
            .setField(SPECIAL_CHAR_BIN)
            .setFilterOperation(FilterOperation.EQ)
            .setIgnoreCase(true)
            .setValue(Value.get(".*"))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SPECIAL_CHAR_SET, null, new Query(ageRangeQualifier));
        StepVerifier.create(flux)
            .verifyComplete();
    }

    @Test
    public void testContainingFindsSquareBracket() {
        String[] specialStrings = new String[]{"[", "$", "\\", "^"};
        for (String specialString : specialStrings) {
            Qualifier ageRangeQualifier = Qualifier.builder()
                .setField(SPECIAL_CHAR_BIN)
                .setFilterOperation(FilterOperation.CONTAINING)
                .setIgnoreCase(true)
                .setValue(Value.get(specialString))
                .build();

            Flux<KeyRecord> flux = queryEngine.select(namespace, SPECIAL_CHAR_SET, null, new Query(ageRangeQualifier));
            StepVerifier.create(flux.collectList())
                .expectNextMatches(results -> {
                    assertThat(results)
                        .allSatisfy(rec -> {
                            String matchStr = rec.record.getString(SPECIAL_CHAR_BIN);
                            assertThat(matchStr).contains(specialString);
                        })
                        .hasSize(1);
                    return true;
                })
                .verifyComplete();
        }
    }

    @Test
    public void stringEqualIgnoreCaseWorksOnIndexedBin() {
        tryCreateIndex(namespace, SET_NAME, "color_index", "color", IndexType.STRING);
        try {
            boolean ignoreCase = true;
            String expectedColor = "blue";

            Qualifier caseInsensitiveQual = Qualifier.builder()
                .setField("color")
                .setFilterOperation(FilterOperation.EQ)
                .setIgnoreCase(ignoreCase)
                .setValue(Value.get("BlUe"))
                .build();

            Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, new Query(caseInsensitiveQual));
            StepVerifier.create(flux.collectList())
                .expectNextMatches(results -> {
                    assertThat(results)
                        .allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(expectedColor))
                        .hasSize(queryEngineTestDataPopulator.colourCounts.get(BLUE));
                    return true;
                })
                .verifyComplete();
        } finally {
            tryDropIndex(namespace, SET_NAME, "color_index");
        }
    }

    @Test
    public void selectWithOrQualifiers() {
        String expectedColor = BLUE;

        // We are  expecting to get back all records where color == blue or (age == 28 || age == 29)
        Qualifier qual1 = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.EQ)
            .setValue(Value.get(expectedColor))
            .build();
        Qualifier qual2 = Qualifier.builder()
            .setField("age")
            .setFilterOperation(FilterOperation.BETWEEN)
            .setValue(Value.get(28))
            .setSecondValue(Value.get(30)) // + 1 as upper limit is exclusive
            .build();

        Qualifier or = Qualifier.or(qual1, qual2);

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, new Query(or));
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                AtomicInteger colorMatched = new AtomicInteger();
                AtomicInteger ageMatched = new AtomicInteger();
                results.forEach(keyRecord -> {
                    int age = keyRecord.record.getInt("age");
                    String color = keyRecord.record.getString("color");

                    assertTrue(expectedColor.equals(color) || (age >= 28 && age <= 29));
                    if (expectedColor.equals(color)) {
                        colorMatched.incrementAndGet();
                    }
                    if ((age >= 28 && age <= 29)) {
                        ageMatched.incrementAndGet();
                    }
                });

                assertThat(colorMatched.get()).isEqualTo(queryEngineTestDataPopulator.colourCounts.get(expectedColor));
                assertThat(ageMatched.get()).isEqualTo(queryEngineTestDataPopulator.ageCount.get(28) +
                    queryEngineTestDataPopulator.ageCount.get(29));

                return true;
            })
            .verifyComplete();
    }

    @Test
    public void selectWithBetweenAndOrQualifiers() {
        Qualifier qualColorIsGreen = Qualifier.builder()
            .setField("color")
            .setFilterOperation(FilterOperation.EQ)
            .setValue(Value.get("green"))
            .build();
        Qualifier qualAgeBetween28And29 = Qualifier.builder()
            .setField("age")
            .setFilterOperation(FilterOperation.BETWEEN)
            .setValue(Value.get(28))
            .setSecondValue(Value.get(30)) // + 1 as upper limit is exclusive
            .build();
        Qualifier qualAgeIs25 = Qualifier.builder()
            .setField("age")
            .setFilterOperation(FilterOperation.EQ)
            .setValue(Value.get(25))
            .build();
        Qualifier qualNameIs696 = Qualifier.builder()
            .setField("name")
            .setFilterOperation(FilterOperation.EQ)
            .setValue(Value.get("name:696"))
            .build();

        Qualifier or = Qualifier.or(qualAgeIs25, qualAgeBetween28And29, qualNameIs696);
        Qualifier or2 = Qualifier.or(qualColorIsGreen, qualNameIs696);
        Qualifier and = Qualifier.and(or, or2);

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, new Query(and));
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                AtomicBoolean has25 = new AtomicBoolean(false);
                results.forEach(keyRecord -> {
                    int age = keyRecord.record.getInt("age");
                    if (age == 25) has25.set(true);
                    else assertTrue("green".equals(keyRecord.record.getString("color"))
                        && age >= 28 && age <= 29);
                });
                assertTrue(has25.get());
                return true;
            })
            .verifyComplete();
    }
}
