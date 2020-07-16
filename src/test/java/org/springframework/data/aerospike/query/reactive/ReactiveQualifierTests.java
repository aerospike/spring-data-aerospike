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
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.query.Qualifier;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.CollectionUtils.countingInt;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.BETWEEN;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.CONTAINING;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.ENDS_WITH;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.EQ;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.GT;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.GTEQ;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.IN;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.LIST_BETWEEN;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.LIST_CONTAINS;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.LT;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.LTEQ;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.MAP_KEYS_BETWEEN;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.MAP_KEYS_CONTAINS;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.MAP_VALUES_BETWEEN;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.MAP_VALUES_CONTAINS;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.START_WITH;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.AGES;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.AGE_COUNTS;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.BLUE;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.COLOURS;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.COLOUR_COUNTS;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.GREEN;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.ORANGE;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.SET_NAME;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.SKIP_LONG_VALUE;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.SPECIAL_CHAR_BIN;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.SPECIAL_CHAR_SET;

/*
 * Tests to ensure that Qualifiers are built successfully for non indexed bins.
 */
public class ReactiveQualifierTests extends BaseReactiveQueryEngineTests {

	/*
	 * These bins should not be indexed.
	 */
	@BeforeEach
	public void dropIndexes() {
		super.tryDropIndex(namespace, SET_NAME, "age_index");
		super.tryDropIndex(namespace, SET_NAME, "color_index");
	}

	@Test
	void throwsExceptionWhenScansDisabled() {
		queryEngine.setScansEnabled(false);
		try {
			Qualifier qualifier = new Qualifier("age", LT, Value.get(26));
			StepVerifier.create(queryEngine.select(namespace, SET_NAME, null, qualifier))
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
		Qualifier AgeRangeQualifier = new Qualifier("age", LT, Value.get(26));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, AgeRangeQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					assertThat(results)
							.filteredOn(keyRecord -> {
								int age = keyRecord.record.getInt("age");
								assertThat(age).isLessThan(26);
								return age == 25;
							})
							.hasSize(AGE_COUNTS.get(25));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void numericLTEQQualifier() {

		// Ages range from 25 -> 29. We expected to only get back values with age <= 26
		Qualifier AgeRangeQualifier = new Qualifier("age", LTEQ, Value.get(26));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, AgeRangeQualifier);
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
					assertThat(age25Count.get()).isEqualTo(AGE_COUNTS.get(25));
					assertThat(age26Count.get()).isEqualTo(AGE_COUNTS.get(26));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void numericEQQualifier() {

		// Ages range from 25 -> 29. We expected to only get back values with age == 26
		Qualifier AgeRangeQualifier = new Qualifier("age", EQ, Value.get(26));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, AgeRangeQualifier);

		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					assertThat(results)
							.allSatisfy(rec -> assertThat(rec.record.getInt("age")).isEqualTo(26))
							.hasSize(AGE_COUNTS.get(26));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void numericGTEQQualifier() {
		// Ages range from 25 -> 29. We expected to only get back values with age >= 28
		Qualifier AgeRangeQualifier = new Qualifier("age", GTEQ, Value.get(28));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, AgeRangeQualifier);
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
					assertThat(age28Count.get()).isEqualTo(AGE_COUNTS.get(25));
					assertThat(age29Count.get()).isEqualTo(AGE_COUNTS.get(26));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void numericGTQualifier() {

		// Ages range from 25 -> 29. We expected to only get back values with age > 28 or equivalently == 29
		Qualifier AgeRangeQualifier = new Qualifier("age", GT, Value.get(28));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, AgeRangeQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					assertThat(results)
							.allSatisfy(rec -> assertThat(rec.record.getInt("age")).isEqualTo(29))
							.hasSize(AGE_COUNTS.get(29));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void stringEQQualifier() {
		Qualifier stringEqQualifier = new Qualifier("color", EQ, Value.get(ORANGE));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, stringEqQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					assertThat(results)
							.allSatisfy(rec -> assertThat(rec.record.getString("color")).endsWith(ORANGE))
							.hasSize(COLOUR_COUNTS.get(ORANGE));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void stringEQQualifierCaseSensitive() {
		Qualifier stringEqQualifier = new Qualifier("color", EQ, true, Value.get(ORANGE.toUpperCase()));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, stringEqQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					assertThat(results)
							.allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(ORANGE))
							.hasSize(COLOUR_COUNTS.get(ORANGE));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void stringStartWithQualifier() {
		String bluePrefix = "blu";

		Qualifier stringEqQualifier = new Qualifier("color", START_WITH, Value.get("blu"));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, stringEqQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					assertThat(results)
							.allSatisfy(rec -> assertThat(rec.record.getString("color")).startsWith(bluePrefix))
							.hasSize(COLOUR_COUNTS.get(BLUE));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void stringStartWithEntireWordQualifier() {
		Qualifier stringEqQualifier = new Qualifier("color", START_WITH, Value.get(BLUE));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, stringEqQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					assertThat(results)
							.allSatisfy(rec -> assertThat(rec.record.getString("color")).startsWith(BLUE))
							.hasSize(COLOUR_COUNTS.get(BLUE));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void stringStartWithICASEQualifier() {
		String blue = "blu";

		Qualifier stringEqQualifier = new Qualifier("color", START_WITH, true, Value.get("BLU"));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, stringEqQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					assertThat(results)
							.allSatisfy(rec -> assertThat(rec.record.getString("color")).startsWith(blue))
							.hasSize(COLOUR_COUNTS.get(BLUE));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void stringEndsWithQualifier() {
		String greenEnding = GREEN.substring(2);

		Qualifier stringEqQualifier = new Qualifier("color", ENDS_WITH, Value.get(greenEnding));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, stringEqQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					assertThat(results)
							.allSatisfy(rec -> assertThat(rec.record.getString("color")).endsWith(greenEnding))
							.hasSize(COLOUR_COUNTS.get(GREEN));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void stringEndsWithEntireWordQualifier() {
		Qualifier stringEqQualifier = new Qualifier("color", ENDS_WITH, Value.get(GREEN));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, stringEqQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					assertThat(results)
							.allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(GREEN))
							.hasSize(COLOUR_COUNTS.get(GREEN));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void betweenQualifier() {
		// Ages range from 25 -> 29. Get back age between 26 and 28 inclusive
		Qualifier AgeRangeQualifier = new Qualifier("age", BETWEEN, Value.get(26), Value.get(28));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, AgeRangeQualifier);
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
					assertThat(age26Count.get()).isEqualTo(AGE_COUNTS.get(26));
					assertThat(age27Count.get()).isEqualTo(AGE_COUNTS.get(27));
					assertThat(age28Count.get()).isEqualTo(AGE_COUNTS.get(28));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void containingQualifier() {
		Map<String, Integer> expectedColorCounts = Arrays.stream(COLOURS)
				.filter(c -> c.contains("l"))
				.collect(Collectors.toMap(c -> c, color -> COLOUR_COUNTS.get(color)));

		Qualifier AgeRangeQualifier = new Qualifier("color", CONTAINING, Value.get("l"));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, AgeRangeQualifier);
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
				.collect(Collectors.toMap(c -> c, color -> COLOUR_COUNTS.get(color)));

		Qualifier qualifier = new Qualifier("color", IN, Value.get(inColours));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, qualifier);
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

		Qualifier AgeRangeQualifier = new Qualifier(binName, LIST_CONTAINS, Value.get(searchColor));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, AgeRangeQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					// Every Record with a color == "color" has a one element list ["color"]
					// so there are an equal amount of records with the list == [lcolor"] as with a color == "color"
					assertThat(results)
							.allSatisfy(rec -> {
								List<String> colorList = (List<String>) rec.record.getList(binName);
								String color = colorList.get(0);
								assertThat(color).isEqualTo(searchColor);
							})
							.hasSize(COLOUR_COUNTS.get(searchColor));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void listBetweenQualifier() {
		int ageStart = AGES[0]; // 25
		int ageEnd = AGES[2]; // 27

		String binName = "longList";

		Qualifier AgeRangeQualifier = new Qualifier(binName, LIST_BETWEEN, Value.get(ageStart), Value.get(ageEnd));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, AgeRangeQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					AtomicInteger age25Count = new AtomicInteger();
					AtomicInteger age26Count = new AtomicInteger();
					AtomicInteger age27Count = new AtomicInteger();
					results.forEach(keyRecord -> {
						int age = keyRecord.record.getInt("age");
						assertThat(age).isBetween(ageStart, ageEnd);
						if (age == 25) {
							age25Count.incrementAndGet();
						} else if (age == 26) {
							age26Count.incrementAndGet();
						} else {
							age27Count.incrementAndGet();
						}
					});
					assertThat(age25Count.get()).isEqualTo(AGE_COUNTS.get(25));
					assertThat(age26Count.get()).isEqualTo(AGE_COUNTS.get(26));
					assertThat(age27Count.get()).isEqualTo(AGE_COUNTS.get(27));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void mapKeysContainsQualifier() {
		String searchColor = COLOURS[0];

		String binName = "colorAgeMap";

		Qualifier AgeRangeQualifier = new Qualifier(binName, MAP_KEYS_CONTAINS, Value.get(searchColor));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, AgeRangeQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					// Every Record with a color == "color" has a one element map {"color" => #}
					// so there are an equal amount of records with the map {"color" => #} as with a color == "color"
					assertThat(results)
							.allSatisfy(rec -> {
								Map<String, ?> colorMap = (Map<String, ?>) rec.record.getMap(binName);
								assertThat(colorMap).containsKey(searchColor);
							})
							.hasSize(COLOUR_COUNTS.get(searchColor));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void testMapValuesContainsQualifier() {
		String searchColor = COLOURS[0];

		String binName = "ageColorMap";

		Qualifier AgeRangeQualifier = new Qualifier(binName, MAP_VALUES_CONTAINS, Value.get(searchColor));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, AgeRangeQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					// Every Record with a color == "color" has a one element map {"color" => #}
					// so there are an equal amount of records with the map {"color" => #} as with a color == "color"
					assertThat(results)
							.allSatisfy(rec -> {
								Map<?, String> colorMap = (Map<?, String>) rec.record.getMap(binName);
								assertThat(colorMap).containsValue(searchColor);
							})
							.hasSize(COLOUR_COUNTS.get(searchColor));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void testMapKeysBetweenQualifier() {
		long ageStart = AGES[0]; // 25
		long ageEnd = AGES[2]; // 27

		String binName = "ageColorMap";

		Qualifier AgeRangeQualifier = new Qualifier(binName, MAP_KEYS_BETWEEN, Value.get(ageStart), Value.get(ageEnd));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, AgeRangeQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					AtomicInteger age25Count = new AtomicInteger();
					AtomicInteger age26Count = new AtomicInteger();
					AtomicInteger age27Count = new AtomicInteger();
					results.forEach(keyRecord -> {
						Map<Long, ?> ageColorMap = (Map<Long, ?>) keyRecord.record.getMap(binName);
						// This is always a one item map
						for (Long age : ageColorMap.keySet()) {
							if (age == SKIP_LONG_VALUE) {
								continue;
							}
							assertThat(age).isBetween(ageStart, ageEnd);
							if (age == 25) {
								age25Count.incrementAndGet();
							} else if (age == 26) {
								age26Count.incrementAndGet();
							} else {
								age27Count.incrementAndGet();
							}
						}
					});
					assertThat(age25Count.get()).isEqualTo(AGE_COUNTS.get(25));
					assertThat(age26Count.get()).isEqualTo(AGE_COUNTS.get(26));
					assertThat(age27Count.get()).isEqualTo(AGE_COUNTS.get(27));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void testMapValuesBetweenQualifier() {
		long ageStart = AGES[0]; // 25
		long ageEnd = AGES[2]; // 27

		String binName = "colorAgeMap";

		Qualifier AgeRangeQualifier = new Qualifier(binName, MAP_VALUES_BETWEEN, Value.get(ageStart), Value.get(ageEnd));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, AgeRangeQualifier);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					AtomicInteger age25Count = new AtomicInteger();
					AtomicInteger age26Count = new AtomicInteger();
					AtomicInteger age27Count = new AtomicInteger();
					results.forEach(keyRecord -> {
						Map<?, Long> colorAgeMap = (Map<?, Long>) keyRecord.record.getMap(binName);
						// This is always a one item map
						for (Long age : colorAgeMap.values()) {
							if (age == SKIP_LONG_VALUE) {
								continue;
							}
							assertThat(age).isBetween(ageStart, ageEnd);
							if (age == 25) {
								age25Count.incrementAndGet();
							} else if (age == 26) {
								age26Count.incrementAndGet();
							} else {
								age27Count.incrementAndGet();
							}
						}
					});
					assertThat(age25Count.get()).isEqualTo(AGE_COUNTS.get(25));
					assertThat(age26Count.get()).isEqualTo(AGE_COUNTS.get(26));
					assertThat(age27Count.get()).isEqualTo(AGE_COUNTS.get(27));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void testContainingDoesNotUseSpecialCharacterQualifier() {
		Qualifier AgeRangeQualifier = new Qualifier(SPECIAL_CHAR_BIN, CONTAINING, Value.get(".*"));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SPECIAL_CHAR_SET, null, AgeRangeQualifier);
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
		Qualifier AgeRangeQualifier = new Qualifier(SPECIAL_CHAR_BIN, START_WITH, Value.get(".*"));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SPECIAL_CHAR_SET, null, AgeRangeQualifier);
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
		Qualifier AgeRangeQualifier = new Qualifier(SPECIAL_CHAR_BIN, ENDS_WITH, Value.get(".*"));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SPECIAL_CHAR_SET, null, AgeRangeQualifier);
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
	public void testEQIcaseDoesNotUseSpecialCharacter() {
		Qualifier AgeRangeQualifier = new Qualifier(SPECIAL_CHAR_BIN, EQ, true, Value.get(".*"));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SPECIAL_CHAR_SET, null, AgeRangeQualifier);
		StepVerifier.create(flux)
				.verifyComplete();
	}

	@Test
	public void testContainingFindsSquareBracket() {

		String[] specialStrings = new String[]{"[", "$", "\\", "^"};
		for (String specialString : specialStrings) {
			Qualifier AgeRangeQualifier = new Qualifier(SPECIAL_CHAR_BIN, CONTAINING, true, Value.get(specialString));
			Flux<KeyRecord> flux = queryEngine.select(namespace, SPECIAL_CHAR_SET, null, AgeRangeQualifier);
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

			Qualifier caseInsensitiveQual = new Qualifier("color", Qualifier.FilterOperation.EQ, ignoreCase, Value.get("BlUe"));
			Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, caseInsensitiveQual);
			StepVerifier.create(flux.collectList())
					.expectNextMatches(results -> {
						assertThat(results)
								.allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(expectedColor))
								.hasSize(COLOUR_COUNTS.get(BLUE));
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
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get(expectedColor));
		Qualifier qual2 = new Qualifier("age", Qualifier.FilterOperation.BETWEEN, Value.get(28), Value.get(29));
		Qualifier or = new Qualifier(Qualifier.FilterOperation.OR, qual1, qual2);
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, or);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					AtomicInteger colorMatched = new AtomicInteger();
					AtomicInteger ageMatched = new AtomicInteger();
					results.forEach(keyRecord -> {
						int age = keyRecord.record.getInt("age");
						String color = keyRecord.record.getString("color");

						Assert.assertTrue(expectedColor.equals(color) || (age >= 28 && age <= 29));
						if (expectedColor.equals(color)) {
							colorMatched.incrementAndGet();
						}
						if ((age >= 28 && age <= 29)) {
							ageMatched.incrementAndGet();
						}
					});

					assertThat(colorMatched.get()).isEqualTo(COLOUR_COUNTS.get(expectedColor));
					assertThat(ageMatched.get()).isEqualTo(AGE_COUNTS.get(28) + AGE_COUNTS.get(29));

					return true;
				})
				.verifyComplete();
	}

	@Test
	public void selectWithBetweenAndOrQualifiers() {
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("green"));
		Qualifier qual2 = new Qualifier("age", Qualifier.FilterOperation.BETWEEN, Value.get(28), Value.get(29));
		Qualifier qual3 = new Qualifier("age", Qualifier.FilterOperation.EQ, Value.get(25));
		Qualifier qual4 = new Qualifier("name", Qualifier.FilterOperation.EQ, Value.get("name:696"));
		Qualifier or = new Qualifier(Qualifier.FilterOperation.OR, qual3, qual2, qual4);
		Qualifier or2 = new Qualifier(Qualifier.FilterOperation.OR, qual1, qual4);
		Qualifier and = new Qualifier(Qualifier.FilterOperation.AND, or, or2);

		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, and);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					AtomicBoolean has25 = new AtomicBoolean(false);
					results.forEach(keyRecord -> {

						int age = keyRecord.record.getInt("age");
						if (age == 25) has25.set(true);
						else Assert.assertTrue("green".equals(keyRecord.record.getString("color"))
								&& (age == 25 || (age >= 28 && age <= 29)));

					});

					Assert.assertTrue(has25.get());

					return true;
				})
				.verifyComplete();
	}
}
