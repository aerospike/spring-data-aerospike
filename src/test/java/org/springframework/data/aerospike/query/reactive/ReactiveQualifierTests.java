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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.*;

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
        reactiveQueryEngine.setScansEnabled(false);
        try {
            Qualifier qualifier = Qualifier.builder()
                .setPath("age")
                .setFilterOperation(FilterOperation.LT)
                .setValue(26)
                .build();

            StepVerifier.create(reactiveQueryEngine.select(namespace, SET_NAME, null, new Query(qualifier)))
                .expectErrorSatisfies(e -> assertThat(e)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("disabled by default"))
                .verify();
        } finally {
            reactiveQueryEngine.setScansEnabled(true);
        }
    }

    @Test
    public void selectAll() {
        Flux<KeyRecord> flux = reactiveQueryEngine.select(namespace, SET_NAME, null, null);

        StepVerifier.create(flux)
            .expectNextCount(RECORD_COUNT)
            .verifyComplete();
    }

    @Test
    public void stringEQQualifier() {
        Qualifier stringEqQualifier = Qualifier.builder()
            .setPath("color")
            .setFilterOperation(FilterOperation.EQ)
            .setValue(ORANGE)
            .build();

        Flux<KeyRecord> flux = reactiveQueryEngine.select(namespace, SET_NAME, null, new Query(stringEqQualifier));
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
            .setPath("color")
            .setFilterOperation(FilterOperation.EQ)
            .setIgnoreCase(true)
            .setValue(ORANGE.toUpperCase())
            .build();

        Flux<KeyRecord> flux = reactiveQueryEngine.select(namespace, SET_NAME, null, new Query(stringEqQualifier));
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
            .setPath("color")
            .setFilterOperation(FilterOperation.STARTS_WITH)
            .setValue("blu")
            .build();

        Flux<KeyRecord> flux = reactiveQueryEngine.select(namespace, SET_NAME, null, new Query(stringEqQualifier));
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
            .setPath("color")
            .setFilterOperation(FilterOperation.STARTS_WITH)
            .setValue(BLUE)
            .build();

        Flux<KeyRecord> flux = reactiveQueryEngine.select(namespace, SET_NAME, null, new Query(stringEqQualifier));
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
            .setPath("color")
            .setFilterOperation(FilterOperation.STARTS_WITH)
            .setIgnoreCase(true)
            .setValue("BLU")
            .build();

        Flux<KeyRecord> flux = reactiveQueryEngine.select(namespace, SET_NAME, null, new Query(stringEqQualifier));
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
            .setPath("color")
            .setFilterOperation(FilterOperation.ENDS_WITH)
            .setValue(greenEnding)
            .build();

        Flux<KeyRecord> flux = reactiveQueryEngine.select(namespace, SET_NAME, null, new Query(stringEqQualifier));
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
    public void betweenQualifier() {
        // Ages range from 25 -> 29. Get back age between 26 and 28 inclusive
        Qualifier ageRangeQualifier = Qualifier.builder()
            .setPath("age")
            .setFilterOperation(FilterOperation.BETWEEN)
            .setValue(26)
            .setSecondValue(29) // + 1 as upper limit is exclusive
            .build();

        Flux<KeyRecord> flux = reactiveQueryEngine.select(namespace, SET_NAME, null, new Query(ageRangeQualifier));
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
    public void listContainsQualifier() {
        String searchColor = COLOURS[0];

        String binName = "colorList";

        Qualifier ageRangeQualifier = Qualifier.builder()
            .setPath(binName)
            .setFilterOperation(FilterOperation.COLLECTION_VAL_CONTAINING)
            .setValue(searchColor)
            .build();

        Flux<KeyRecord> flux = reactiveQueryEngine.select(namespace, SET_NAME, null, new Query(ageRangeQualifier));
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
            .setPath(binName)
            .setFilterOperation(FilterOperation.MAP_KEYS_CONTAIN)
            .setValue(searchColor)
            .build();

        Flux<KeyRecord> flux = reactiveQueryEngine.select(namespace, SET_NAME, null, new Query(ageRangeQualifier));
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
    public void testContainingDoesNotUseSpecialCharacterQualifier() {
        Qualifier ageRangeQualifier = Qualifier.builder()
            .setPath(SPECIAL_CHAR_BIN)
            .setFilterOperation(FilterOperation.CONTAINING)
            .setValue(".*")
            .build();

        Flux<KeyRecord> flux = reactiveQueryEngine.select(namespace, SPECIAL_CHAR_SET, null,
            new Query(ageRangeQualifier));
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
            .setPath(SPECIAL_CHAR_BIN)
            .setFilterOperation(FilterOperation.STARTS_WITH)
            .setValue(".*")
            .build();

        Flux<KeyRecord> flux = reactiveQueryEngine.select(namespace, SPECIAL_CHAR_SET, null,
            new Query(ageRangeQualifier));
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
    public void testEQICaseDoesNotUseSpecialCharacter() {
        Qualifier ageRangeQualifier = Qualifier.builder()
            .setPath(SPECIAL_CHAR_BIN)
            .setFilterOperation(FilterOperation.EQ)
            .setIgnoreCase(true)
            .setValue(".*")
            .build();

        Flux<KeyRecord> flux = reactiveQueryEngine.select(namespace, SPECIAL_CHAR_SET, null,
            new Query(ageRangeQualifier));
        StepVerifier.create(flux)
            .verifyComplete();
    }

    @Test
    public void testContainingFindsSquareBracket() {
        String[] specialStrings = new String[]{"[", "$", "\\", "^"};
        for (String specialString : specialStrings) {
            Qualifier ageRangeQualifier = Qualifier.builder()
                .setPath(SPECIAL_CHAR_BIN)
                .setFilterOperation(FilterOperation.CONTAINING)
                .setIgnoreCase(true)
                .setValue(specialString)
                .build();

            Flux<KeyRecord> flux = reactiveQueryEngine.select(namespace, SPECIAL_CHAR_SET, null,
                new Query(ageRangeQualifier));
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
                .setPath("color")
                .setFilterOperation(FilterOperation.EQ)
                .setIgnoreCase(ignoreCase)
                .setValue("BlUe")
                .build();

            Flux<KeyRecord> flux = reactiveQueryEngine.select(namespace, SET_NAME, null,
                new Query(caseInsensitiveQual));
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
    public void selectWithGeoWithin() {
        double lon = -122.0;
        double lat = 37.5;
        double radius = 50000.0;
        String rgnstr = String.format("{ \"type\": \"AeroCircle\", "
                + "\"coordinates\": [[%.8f, %.8f], %f] }",
            lon, lat, radius);
        Qualifier qual1 = Qualifier.builder()
            .setPath(GEO_BIN_NAME)
            .setFilterOperation(FilterOperation.GEO_WITHIN)
            .setValue(Value.getAsGeoJSON(rgnstr))
            .build();

        Flux<KeyRecord> flux = reactiveQueryEngine.select(namespace, GEO_SET, null, new Query(qual1));
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                assertThat(results)
                    .allSatisfy(rec -> assertThat(rec.record.generation).isPositive())
                    .isNotEmpty();
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void selectWithOrQualifiers() {
        String expectedColor = BLUE;

        // We are  expecting to get back all records where color == blue or (age == 28 || age == 29)
        Qualifier qual1 = Qualifier.builder()
            .setPath("color")
            .setFilterOperation(FilterOperation.EQ)
            .setValue(expectedColor)
            .build();
        Qualifier qual2 = Qualifier.builder()
            .setPath("age")
            .setFilterOperation(FilterOperation.BETWEEN)
            .setValue(28)
            .setSecondValue(30) // + 1 as upper limit is exclusive
            .build();

        Qualifier or = Qualifier.or(qual1, qual2);

        Flux<KeyRecord> flux = reactiveQueryEngine.select(namespace, SET_NAME, null, new Query(or));
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
            .setPath("color")
            .setFilterOperation(FilterOperation.EQ)
            .setValue("green")
            .build();
        Qualifier qualAgeBetween28And29 = Qualifier.builder()
            .setPath("age")
            .setFilterOperation(FilterOperation.BETWEEN)
            .setValue(28)
            .setSecondValue(30) // + 1 as upper limit is exclusive
            .build();
        Qualifier qualAgeIs25 = Qualifier.builder()
            .setPath("age")
            .setFilterOperation(FilterOperation.EQ)
            .setValue(25)
            .build();
        Qualifier qualNameIs696 = Qualifier.builder()
            .setPath("name")
            .setFilterOperation(FilterOperation.EQ)
            .setValue("name:696")
            .build();

        Qualifier or = Qualifier.or(qualAgeIs25, qualAgeBetween28And29, qualNameIs696);
        Qualifier or2 = Qualifier.or(qualColorIsGreen, qualNameIs696);
        Qualifier and = Qualifier.and(or, or2);

        Flux<KeyRecord> flux = reactiveQueryEngine.select(namespace, SET_NAME, null, new Query(and));
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
