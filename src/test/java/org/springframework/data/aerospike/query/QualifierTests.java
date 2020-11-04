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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.aerospike.CollectionUtils;
import org.springframework.data.aerospike.query.Qualifier.FilterOperation;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.CollectionUtils.countingInt;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.LT;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.AGES;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.BLUE;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.COLOURS;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.GEO_BIN_NAME;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.GEO_SET;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.GREEN;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.ORANGE;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.RECORD_COUNT;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.SET_NAME;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.SKIP_LONG_VALUE;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.SPECIAL_CHAR_BIN;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.SPECIAL_CHAR_SET;

/*
 * Tests to ensure that Qualifiers are built successfully for non indexed bins.
 */
public class QualifierTests extends BaseQueryEngineTests {

	/*
	 * These bins should not be indexed.
	 */
	@BeforeEach
	public void dropIndexes() {
		tryDropIndex(namespace, SET_NAME, "age_index");
		tryDropIndex(namespace, SET_NAME, "color_index");
	}

	@Test
	void throwsExceptionWhenScansDisabled() {
		queryEngine.setScansEnabled(false);
		try {
			Qualifier qualifier = new Qualifier("age", LT, Value.get(26));
			assertThatThrownBy(() -> queryEngine.select(namespace, SET_NAME, null, qualifier))
					.isInstanceOf(IllegalStateException.class)
					.hasMessageContaining("disabled by default");
		} finally {
			queryEngine.setScansEnabled(true);
		}
	}

	@Test
	public void selectOneWitKey() {
		KeyQualifier kq = new KeyQualifier(Value.get("selector-test:3"));

		KeyRecordIterator iterator = queryEngine.select(namespace, SET_NAME, null, kq);

		assertThat(iterator).toIterable().hasSize(1);
	}

	@Test
	public void selectOneWitKeyNonExisting() {
		KeyQualifier kq = new KeyQualifier(Value.get("selector-test:unknown"));

		KeyRecordIterator iterator = queryEngine.select(namespace, SET_NAME, null, kq);

		assertThat(iterator).toIterable().isEmpty();
	}

	@Test
	public void selectAll() {
		KeyRecordIterator iterator = queryEngine.select(namespace, SET_NAME, null);

		assertThat(iterator).toIterable().hasSize(RECORD_COUNT);
	}

	@Test
	public void lTQualifier() {
		// Ages range from 25 -> 29. We expected to only get back values with age < 26

		Qualifier qualifier = new Qualifier("age", FilterOperation.LT, Value.get(26));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

		assertThat(it)
				.toIterable()
				.isNotEmpty()
				.allSatisfy(rec -> assertThat(rec.record.getInt("age")).isLessThan(26))
				.hasSize(queryEngineTestDataPopulator.ageCount.get(25));
	}

	@Test
	public void numericLTEQQualifier() {
		// Ages range from 25 -> 29. We expected to only get back values with age <= 26

		Qualifier qualifier = new Qualifier("age", FilterOperation.LTEQ, Value.get(26));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

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
	public void numericEQQualifier() {
		// Ages range from 25 -> 29. We expected to only get back values with age == 26

		Qualifier qualifier = new Qualifier("age", FilterOperation.EQ, Value.get(26));
		KeyRecordIterator iterator = queryEngine.select(namespace, SET_NAME, null, qualifier);

		assertThat(iterator)
				.toIterable()
				.isNotEmpty()
				.allSatisfy(rec -> assertThat(rec.record.getInt("age")).isEqualTo(26))
				.hasSize(queryEngineTestDataPopulator.ageCount.get(26));
	}

	@Test
	public void numericGTEQQualifier() {
		// Ages range from 25 -> 29. We expected to only get back values with age >= 28

		Qualifier qualifier = new Qualifier("age", FilterOperation.GTEQ, Value.get(28));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

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
	public void numericGTQualifier() {
		// Ages range from 25 -> 29. We expected to only get back values with age > 28 or equivalently == 29

		Qualifier qualifier = new Qualifier("age", FilterOperation.GT, Value.get(28));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

		assertThat(it)
				.toIterable()
				.isNotEmpty()
				.allSatisfy(rec -> assertThat(rec.record.getInt("age")).isEqualTo(29))
				.hasSize(queryEngineTestDataPopulator.ageCount.get(29));
	}

	@Test
	public void stringEQQualifier() {

		Qualifier qualifier = new Qualifier("color", FilterOperation.EQ, Value.get(ORANGE));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

		assertThat(it)
				.toIterable()
				.isNotEmpty()
				.allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(ORANGE))
				.hasSize(queryEngineTestDataPopulator.colourCounts.get(ORANGE));
	}

	@Test
	public void stringEQIgnoreCaseQualifier() {

		Qualifier qualifier = new Qualifier("color", FilterOperation.EQ, true, Value.get(ORANGE.toUpperCase()));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

		assertThat(it)
				.toIterable()
				.isNotEmpty()
				.allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(ORANGE))
				.hasSize(queryEngineTestDataPopulator.colourCounts.get(ORANGE));
	}

	@Test
	public void stringEqualIgnoreCaseWorksOnUnindexedBin() {
		boolean ignoreCase = true;
		Qualifier qualifier = new Qualifier("color", FilterOperation.EQ, ignoreCase, Value.get("BlUe"));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

		assertThat(it)
				.toIterable()
				.isNotEmpty()
				.allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(BLUE))
				.hasSize(queryEngineTestDataPopulator.colourCounts.get(BLUE));
	}

	@Test
	public void stringEqualIgnoreCaseWorksOnIndexedBin() {
		withIndex(namespace, SET_NAME, "color_index_selector", "color", IndexType.STRING, () -> {
			boolean ignoreCase = true;
			Qualifier qualifier = new Qualifier("color", Qualifier.FilterOperation.EQ, ignoreCase, Value.get("BlUe"));
			KeyRecordIterator iterator = queryEngine.select(namespace, SET_NAME, null, qualifier);

			assertThat(iterator)
					.toIterable()
					.isNotEmpty()
					.allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(BLUE))
					.hasSize(queryEngineTestDataPopulator.colourCounts.get(BLUE));
			// scan will be run, since Aerospike filter does not support case-insensitive string comparison
		});
	}

	@Test
	public void stringEqualIgnoreCaseWorksRequiresFullMatch() {
		boolean ignoreCase = true;
		Qualifier qualifier = new Qualifier("color", FilterOperation.EQ, ignoreCase, Value.get("lue"));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

		assertThat(it).toIterable().isEmpty();
	}

	@Test
	public void stringStartWithQualifier() {

		Qualifier qualifier = new Qualifier("color", FilterOperation.START_WITH, Value.get(BLUE.substring(0, 2)));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

		assertThat(it)
				.toIterable()
				.isNotEmpty()
				.allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(BLUE))
				.hasSize(queryEngineTestDataPopulator.colourCounts.get(BLUE));
	}

	@Test
	public void stringStartWithEntireWordQualifier() {

		Qualifier qualifier = new Qualifier("color", FilterOperation.START_WITH, Value.get(BLUE));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

		assertThat(it)
				.toIterable()
				.isNotEmpty()
				.allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(BLUE))
				.hasSize(queryEngineTestDataPopulator.colourCounts.get(BLUE));
	}

	@Test
	public void stringStartWithICASEQualifier() {

		Qualifier qualifier = new Qualifier("color", FilterOperation.START_WITH, true, Value.get("BLU"));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

		assertThat(it)
				.toIterable()
				.isNotEmpty()
				.allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(BLUE))
				.hasSize(queryEngineTestDataPopulator.colourCounts.get(BLUE));
	}

	@Test
	public void stringEndsWithQualifier() {

		Qualifier qualifier = new Qualifier("color", FilterOperation.ENDS_WITH, Value.get(GREEN.substring(2)));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

		assertThat(it)
				.toIterable()
				.isNotEmpty()
				.allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(GREEN))
				.hasSize(queryEngineTestDataPopulator.colourCounts.get(GREEN));
	}

	@Test
	public void selectEndsWith() {

		Qualifier qualifier = new Qualifier("color", FilterOperation.ENDS_WITH, Value.get("e"));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

		assertThat(it)
				.toIterable()
				.isNotEmpty()
				.allSatisfy(rec -> assertThat(rec.record.getString("color")).isIn(BLUE, ORANGE))
				.hasSize(queryEngineTestDataPopulator.colourCounts.get(BLUE) + queryEngineTestDataPopulator.colourCounts.get(ORANGE));
	}

	@Test
	public void stringEndsWithEntireWordQualifier() {

		Qualifier qualifier = new Qualifier("color", FilterOperation.ENDS_WITH, Value.get(GREEN));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

		assertThat(it)
				.toIterable()
				.isNotEmpty()
				.allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(GREEN))
				.hasSize(queryEngineTestDataPopulator.colourCounts.get(GREEN));
	}

	@Test
	public void betweenQualifier() {
		// Ages range from 25 -> 29. Get back age between 26 and 28 inclusive

		Qualifier qualifier = new Qualifier("age", FilterOperation.BETWEEN, Value.get(26), Value.get(28));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

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
	public void containingQualifier() {
		Map<String, Integer> expectedCounts = Arrays.stream(COLOURS)
				.filter(c -> c.contains("l"))
				.collect(Collectors.toMap(color -> color, color -> queryEngineTestDataPopulator.colourCounts.get(color)));

		Qualifier qualifier = new Qualifier("color", FilterOperation.CONTAINING, Value.get("l"));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

		Map<String, Integer> colorCount = CollectionUtils.toStream(it)
				.map(rec -> rec.record.getString("color"))
				.collect(Collectors.groupingBy(k -> k, countingInt()));
		assertThat(colorCount).isNotEmpty().isEqualTo(expectedCounts);
	}

	@Test
	public void inQualifier() {
		List<String> inColors = Arrays.asList(COLOURS[0], COLOURS[2]);
		Map<String, Integer> expectedCounts = inColors.stream()
				.collect(Collectors.toMap(color -> color, color -> queryEngineTestDataPopulator.colourCounts.get(color)));

		Qualifier qualifier = new Qualifier("color", FilterOperation.IN, Value.get(inColors));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

		Map<String, Integer> colorCount = CollectionUtils.toStream(it)
				.map(rec -> rec.record.getString("color"))
				.collect(Collectors.groupingBy(k -> k, countingInt()));
		assertThat(colorCount).isNotEmpty().isEqualTo(expectedCounts);
	}

	@Test
	public void listContainsQualifier() {
		String searchColor = COLOURS[0];
		String binName = "colorList";

		Qualifier qualifier = new Qualifier(binName, FilterOperation.LIST_CONTAINS, Value.get(searchColor));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

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
	public void listBetweenQualifier() {
		long ageStart = AGES[0]; // 25
		long ageEnd = AGES[2]; // 27
		String binName = "longList";

		Qualifier qualifier = new Qualifier(binName, FilterOperation.LIST_BETWEEN, Value.get(ageStart), Value.get(ageEnd));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

		Map<Long, Integer> ageCount = CollectionUtils.toStream(it)
				.map(rec -> {
					List<Long> ageList = (List<Long>) rec.record.getList(binName);
					Long age = ageList.get(0);
					return age;
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
	public void mapKeysContainsQualifier() {
		String searchColor = COLOURS[0];
		String binName = "colorAgeMap";

		Qualifier qualifier = new Qualifier(binName, FilterOperation.MAP_KEYS_CONTAINS, Value.get(searchColor));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

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
	public void mapValuesContainsQualifier() {
		String searchColor = COLOURS[0];
		String binName = "ageColorMap";

		Qualifier qualifier = new Qualifier(binName, FilterOperation.MAP_VALUES_CONTAINS, Value.get(searchColor));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

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
	public void mapKeysBetweenQualifier() {
		long ageStart = AGES[0]; // 25
		long ageEnd = AGES[2]; // 27
		String binName = "ageColorMap";

		Qualifier qualifier = new Qualifier(binName, FilterOperation.MAP_KEYS_BETWEEN, Value.get(ageStart), Value.get(ageEnd));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

		Map<Long, Integer> ageCount = CollectionUtils.toStream(it)
				.map(rec -> {
					@SuppressWarnings("unchecked")
					Map<Long, ?> ageColorMap = (Map<Long, ?>) rec.record.getMap(binName);
					// This is always a one item map
					Long age = ageColorMap.keySet().stream().filter(val -> val != SKIP_LONG_VALUE).findFirst().get();
					return age;
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
	public void mapValuesBetweenQualifier() {
		long ageStart = AGES[0]; // 25
		long ageEnd = AGES[2]; // 27
		String binName = "colorAgeMap";

		Qualifier qualifier = new Qualifier(binName, FilterOperation.MAP_VALUES_BETWEEN, Value.get(ageStart), Value.get(ageEnd));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

		Map<Long, Integer> ageCount = CollectionUtils.toStream(it)
				.map(rec -> {
					@SuppressWarnings("unchecked")
					Map<?, Long> ageColorMap = (Map<?, Long>) rec.record.getMap(binName);
					// This is always a one item map
					Long age = ageColorMap.values().stream().filter(val -> val != SKIP_LONG_VALUE).findFirst().get();
					return age;
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
	public void containingDoesNotUseSpecialCharacterQualifier() {

		Qualifier qualifier = new Qualifier(SPECIAL_CHAR_BIN, FilterOperation.CONTAINING, Value.get(".*"));
		KeyRecordIterator it = queryEngine.select(namespace, SPECIAL_CHAR_SET, null, qualifier);

		assertThat(it).toIterable()
				.isNotEmpty()
				.allSatisfy(rec -> assertThat(rec.record.getString(SPECIAL_CHAR_BIN)).contains(".*"))
				.hasSize(3);
	}

	@Test
	public void startWithDoesNotUseSpecialCharacterQualifier() {

		Qualifier qualifier = new Qualifier(SPECIAL_CHAR_BIN, FilterOperation.START_WITH, Value.get(".*"));
		KeyRecordIterator it = queryEngine.select(namespace, SPECIAL_CHAR_SET, null, qualifier);

		assertThat(it).toIterable()
				.isNotEmpty()
				.allSatisfy(rec -> assertThat(rec.record.getString(SPECIAL_CHAR_BIN)).startsWith(".*"))
				.hasSize(1);
	}

	@Test
	public void endWithDoesNotUseSpecialCharacterQualifier() {

		Qualifier qualifier = new Qualifier(SPECIAL_CHAR_BIN, FilterOperation.ENDS_WITH, Value.get(".*"));
		KeyRecordIterator it = queryEngine.select(namespace, SPECIAL_CHAR_SET, null, qualifier);

		assertThat(it).toIterable()
				.isNotEmpty()
				.allSatisfy(rec -> assertThat(rec.record.getString(SPECIAL_CHAR_BIN)).endsWith(".*"))
				.hasSize(1);
	}

	@Test
	public void eQIcaseDoesNotUseSpecialCharacter() {

		Qualifier qualifier = new Qualifier(SPECIAL_CHAR_BIN, FilterOperation.EQ, true, Value.get(".*"));
		KeyRecordIterator it = queryEngine.select(namespace, SPECIAL_CHAR_SET, null, qualifier);

		assertThat(it).toIterable().isEmpty();
	}

	@ParameterizedTest
	@ValueSource(strings = {"[", "$", "\\", "^"})
	public void containingFindsSquareBracket(String specialString) {

		Qualifier qualifier = new Qualifier(SPECIAL_CHAR_BIN, FilterOperation.CONTAINING, true, Value.get(specialString));
		KeyRecordIterator it = queryEngine.select(namespace, SPECIAL_CHAR_SET, null, qualifier);

		assertThat(it).toIterable()
				.isNotEmpty()
				.allSatisfy(rec -> assertThat(rec.record.getString(SPECIAL_CHAR_BIN)).contains(specialString))
				.hasSize(1);
	}

	@Test
	public void selectWithGeoWithin() {
		double lon = -122.0;
		double lat = 37.5;
		double radius = 50000.0;
		String rgnstr = String.format("{ \"type\": \"AeroCircle\", "
						+ "\"coordinates\": [[%.8f, %.8f], %f] }",
				lon, lat, radius);
		Qualifier qualifier = new Qualifier(GEO_BIN_NAME, FilterOperation.GEO_WITHIN, Value.getAsGeoJSON(rgnstr));
		KeyRecordIterator iterator = queryEngine.select(namespace, GEO_SET, null, qualifier);

		assertThat(iterator).toIterable()
				.isNotEmpty()
				.allSatisfy(rec -> assertThat(rec.record.generation).isGreaterThanOrEqualTo(1));
	}

	@Test
	public void startWithAndEqualIgnoreCaseReturnsAllItems() {

		boolean ignoreCase = true;
		Qualifier qual1 = new Qualifier("color", FilterOperation.EQ, ignoreCase, Value.get(BLUE.toUpperCase()));
		Qualifier qual2 = new Qualifier("name", FilterOperation.START_WITH, ignoreCase, Value.get("NA"));

		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qual1, qual2);

		assertThat(it).toIterable()
				.isNotEmpty()
				.allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(BLUE))
				.hasSize(queryEngineTestDataPopulator.colourCounts.get(BLUE));
	}

	@Test
	public void equalIgnoreCaseReturnsNoItemsIfNoneMatched() {

		boolean ignoreCase = false;
		Qualifier qual1 = new Qualifier("color", FilterOperation.EQ, ignoreCase, Value.get(BLUE.toUpperCase()));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qual1);

		assertThat(it).toIterable().isEmpty();
	}

	@Test
	public void startWithIgnoreCaseReturnsNoItemsIfNoneMatched() {

		boolean ignoreCase = false;
		Qualifier qual1 = new Qualifier("name", FilterOperation.START_WITH, ignoreCase, Value.get("NA"));
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qual1);

		assertThat(it).toIterable().isEmpty();
	}

	@Test
	public void selectWithBetweenAndOrQualifiers() {

		Qualifier colorIsGreen = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get(GREEN));
		Qualifier ageBetween28And29 = new Qualifier("age", Qualifier.FilterOperation.BETWEEN, Value.get(28), Value.get(29));
		Qualifier ageIs25 = new Qualifier("age", Qualifier.FilterOperation.EQ, Value.get(25));
		Qualifier nameIs696 = new Qualifier("name", Qualifier.FilterOperation.EQ, Value.get("name:696"));
		Qualifier or = new Qualifier(Qualifier.FilterOperation.OR, ageIs25, ageBetween28And29, nameIs696);
		Qualifier or2 = new Qualifier(Qualifier.FilterOperation.OR, colorIsGreen, nameIs696);
		Qualifier qualifier = new Qualifier(Qualifier.FilterOperation.AND, or, or2);
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, qualifier);

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
	public void selectWithOrQualifiers() {

		// We are  expecting to get back all records where color == blue or (age == 28 || age == 29)
		Qualifier colorIsBlue = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get(BLUE));
		Qualifier ageBetween28And29 = new Qualifier("age", Qualifier.FilterOperation.BETWEEN, Value.get(28), Value.get(29));
		Qualifier or = new Qualifier(Qualifier.FilterOperation.OR, colorIsBlue, ageBetween28And29);
		KeyRecordIterator it = queryEngine.select(namespace, SET_NAME, null, or);

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
