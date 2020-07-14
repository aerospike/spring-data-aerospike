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

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.query.KeyRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.query.Qualifier.FilterOperation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.assertj.core.api.Assertions.assertThat;

/*
 * Tests to ensure that Qualifiers are built successfully for non indexed bins.
 */
public class QualifierTests extends BaseQueryEngineTests {

	/*
	 * These bins should not be indexed.
	 */
	@BeforeEach
	public void dropIndexes() {
		tryDropIndex(namespace, BaseQueryEngineTests.SET_NAME, "age_index");
		tryDropIndex(namespace, BaseQueryEngineTests.SET_NAME, "color_index");

		Key ewsKey = new Key(namespace, BaseQueryEngineTests.SET_NAME, "ends-with-star");
		Bin ewsBin = new Bin(specialCharBin, "abcd.*");
		this.client.put(null, ewsKey, ewsBin);

		Key swsKey = new Key(namespace, BaseQueryEngineTests.SET_NAME, "starts-with-star");
		Bin swsBin = new Bin(specialCharBin, ".*abcd");
		this.client.put(null, swsKey, swsBin);

		Key starKey = new Key(namespace, BaseQueryEngineTests.SET_NAME, "mid-with-star");
		Bin starBin = new Bin(specialCharBin, "a.*b");
		this.client.put(null, starKey, starBin);

		Key specialCharKey = new Key(namespace, BaseQueryEngineTests.SET_NAME, "special-chars");
		Bin specialCharsBin = new Bin(specialCharBin, "a[$^\\ab");
		this.client.put(null, specialCharKey, specialCharsBin);
	}

	@Test
	public void testLTQualifier() {
		int age25Count = 0;
		// Ages range from 25 -> 29. We expected to only get back values with age < 26
		Qualifier qualifier = new Qualifier("age", FilterOperation.LT, Value.get(26));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				int age = rec.record.getInt("age");

				if (age == 25) {
					age25Count++;
				}
				assertThat(age).isLessThan(26);
			}
		}
		// Make sure that our query returned all of the records we expected.
		assertThat(age25Count).isEqualTo(recordsWithAgeCounts.get(25));
	}

	@Test
	public void testNumericLTEQQualifier() {
		int age25Count = 0;
		int age26Count = 0;

		// Ages range from 25 -> 29. We expected to only get back values with age <= 26
		Qualifier qualifier = new Qualifier("age", FilterOperation.LTEQ, Value.get(26));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				int age = rec.record.getInt("age");

				if (age == 25) {
					age25Count++;
				} else if (age == 26) {
					age26Count++;
				}
				assertThat(age).isLessThanOrEqualTo(26);
			}
		}

		// Make sure that our query returned all of the records we expected.
		assertThat(age25Count).isEqualTo(recordsWithAgeCounts.get(25));
		assertThat(age26Count).isEqualTo(recordsWithAgeCounts.get(26));
	}

	@Test
	public void testNumericEQQualifier() {
		int age26Count = 0;

		// Ages range from 25 -> 29. We expected to only get back values with age == 26
		Qualifier qualifier = new Qualifier("age", FilterOperation.EQ, Value.get(26));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				int age = rec.record.getInt("age");
				if (age == 26) {
					age26Count++;
				}
				assertThat(age).isEqualTo(26);
			}
		}

		// Make sure that our query returned all of the records we expected.
		assertThat(age26Count).isEqualTo(recordsWithAgeCounts.get(26));
	}

	@Test
	public void testNumericGTEQQualifier() {
		int age28Count = 0;
		int age29Count = 0;

		// Ages range from 25 -> 29. We expected to only get back values with age >= 28
		Qualifier qualifier = new Qualifier("age", FilterOperation.GTEQ, Value.get(28));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				int age = rec.record.getInt("age");

				if (age == 28) {
					age28Count++;
				} else if (age == 29) {
					age29Count++;
				}
				assertThat(age).isGreaterThanOrEqualTo(28);
			}
		}

		assertThat(age28Count).isEqualTo(recordsWithAgeCounts.get(28));
		assertThat(age29Count).isEqualTo(recordsWithAgeCounts.get(29));
	}

	@Test
	public void testNumericGTQualifier() {
		int age29Count = 0;

		// Ages range from 25 -> 29. We expected to only get back values with age > 28 or equivalently == 29
		Qualifier qualifier = new Qualifier("age", FilterOperation.GT, Value.get(28));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				int age = rec.record.getInt("age");
				assertThat(age).isEqualTo(29);
				age29Count++;
			}
		}

		assertThat(age29Count).isEqualTo(recordsWithAgeCounts.get(29));
	}

	@Test
	public void testStringEQQualifier() {
		int orangeCount = 0;

		Qualifier qualifier = new Qualifier("color", FilterOperation.EQ, Value.get(ORANGE));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				String color = rec.record.getString("color");
				if (ORANGE.equals(color)) {
					orangeCount++;
				}
				assertThat(color).isEqualTo(ORANGE);
			}
		}

		// Make sure that our query returned all of the records we expected.
		assertThat(orangeCount).isEqualTo(recordsWithColourCounts.get(ORANGE));
	}

	@Test
	public void testStringEQIgnoreCaseQualifier() {
		int orangeCount = 0;

		Qualifier qualifier = new Qualifier("color", FilterOperation.EQ, true, Value.get(ORANGE.toUpperCase()));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				assertThat(rec.record.getString("color")).isEqualToIgnoringCase(ORANGE);
				orangeCount++;
			}
		}
		// Make sure that our query returned all of the records we expected.
		assertThat(orangeCount).isEqualTo(recordsWithColourCounts.get(ORANGE));
	}

	@Test
	public void testStringStartWithQualifier() {
		int blueCount = 0;
		String bluePrefix = "blu";

		Qualifier qualifier = new Qualifier("color", FilterOperation.START_WITH, Value.get("blu"));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				String color = rec.record.getString("color");
				if (color.equals(BLUE)) {
					blueCount++;
				}
				assertThat(color.startsWith(bluePrefix)).isTrue();
			}
		}
		// Make sure that our query returned all of the records we expected.
		assertThat(blueCount).isEqualTo(recordsWithColourCounts.get(BLUE));
	}

	@Test
	public void testStringStartWithEntireWordQualifier() {
		int blueCount = 0;

		Qualifier qualifier = new Qualifier("color", FilterOperation.START_WITH, Value.get(BLUE));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				String color = rec.record.getString("color");
				if (color.equals(BLUE)) {
					blueCount++;
				}
				assertThat(color.startsWith(BLUE)).isTrue();
			}
		}
		// Make sure that our query returned all of the records we expected.
		assertThat(blueCount).isEqualTo(recordsWithColourCounts.get(BLUE));
	}

	@Test
	public void testStringStartWithICASEQualifier() {
		int blueCount = 0;
		String blue = "blu";

		Qualifier qualifier = new Qualifier("color", FilterOperation.START_WITH, true, Value.get("BLU"));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				String color = rec.record.getString("color");
				if (color.equals(BLUE)) {
					blueCount++;
				}
				assertThat(color.startsWith(blue)).isTrue();
			}
		}
		// Make sure that our query returned all of the records we expected.
		assertThat(blueCount).isEqualTo(recordsWithColourCounts.get(BLUE));
	}

	@Test
	public void testStringEndsWithQualifier() {
		int greenCount = 0;
		String greenEnding = GREEN.substring(2);

		Qualifier qualifier = new Qualifier("color", FilterOperation.ENDS_WITH, Value.get(greenEnding));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				String color = rec.record.getString("color");
				if (color.equals(GREEN)) {
					greenCount++;
				}
				assertThat(color.endsWith(greenEnding)).isTrue();
			}
		}
		// Make sure that our query returned all of the records we expected.
		assertThat(greenCount).isEqualTo(recordsWithColourCounts.get(GREEN));
	}

	@Test
	public void testStringEndsWithEntireWordQualifier() {
		int greenCount = 0;

		Qualifier qualifier = new Qualifier("color", FilterOperation.ENDS_WITH, Value.get(QualifierTests.GREEN));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				String color = rec.record.getString("color");
				if (color.equals(QualifierTests.GREEN)) {
					greenCount++;
				}
				assertThat(color).isEqualTo(QualifierTests.GREEN);
			}
		}
		// Make sure that our query returned all of the records we expected.
		assertThat(greenCount).isEqualTo(recordsWithColourCounts.get(QualifierTests.GREEN));
	}

	@Test
	public void testBetweenQualifier() {
		int age26Count = 0;
		int age27Count = 0;
		int age28Count = 0;
		// Ages range from 25 -> 29. Get back age between 26 and 28 inclusive
		Qualifier qualifier = new Qualifier("age", FilterOperation.BETWEEN, Value.get(26), Value.get(28));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				int age = rec.record.getInt("age");
				assertThat(age).isBetween(26, 28);
				if (age == 26) {
					age26Count++;
				} else if (age == 27) {
					age27Count++;
				} else {
					age28Count++;
				}
			}
		}

		assertThat(age26Count).isEqualTo(recordsWithAgeCounts.get(26));
		assertThat(age27Count).isEqualTo(recordsWithAgeCounts.get(27));
		assertThat(age28Count).isEqualTo(recordsWithAgeCounts.get(28));
	}

	@Test
	public void testContainingQualifier() {
		String[] hasLColors = Arrays.stream(colours)
				.filter(c -> c.contains("l")).toArray(String[]::new);

		Map<String, Integer> lColorCounts = new HashMap<>();
		for (String color : hasLColors) {
			lColorCounts.put(color, 0);
		}

		Qualifier qualifier = new Qualifier("color", FilterOperation.CONTAINING, Value.get("l"));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				String color = rec.record.getString("color");
				assertThat(lColorCounts).containsKey(color);
				lColorCounts.put(color, lColorCounts.get(color) + 1);
			}
		}

		for (Entry<String, Integer> colorCountEntry : lColorCounts.entrySet()) {
			assertThat(colorCountEntry.getValue()).isEqualTo(recordsWithColourCounts.get(colorCountEntry.getKey()));
		}
	}

	@Test
	public void testInQualifier() {
		String[] inColours = new String[]{colours[0], colours[2]};

		Map<String, Integer> lColorCounts = new HashMap<>();
		for (String color : inColours) {
			lColorCounts.put(color, 0);
		}

		Qualifier qualifier = new Qualifier("color", FilterOperation.IN, Value.get(Arrays.asList(inColours)));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				String color = rec.record.getString("color");
				assertThat(lColorCounts).containsKey(color);
				lColorCounts.put(color, lColorCounts.get(color) + 1);
			}
		}

		for (Entry<String, Integer> colorCountEntry : lColorCounts.entrySet()) {
			assertThat(colorCountEntry.getValue()).isEqualTo(recordsWithColourCounts.get(colorCountEntry.getKey()));
		}
	}

	@Test
	public void testListContainsQualifier() {
		String searchColor = colours[0];
		int colorCount = 0;

		String binName = "colorList";

		Qualifier qualifier = new Qualifier(binName, FilterOperation.LIST_CONTAINS, Value.get(searchColor));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				@SuppressWarnings("unchecked")
				List<String> colorList = (List<String>) rec.record.getList(binName);
				String color = colorList.get(0);
				assertThat(color).isEqualTo(searchColor);
				colorCount++;
			}
		}

		// Every Record with a color == "color" has a one element list ["color"]
		// so there are an equal amount of records with the list == [lcolor"] as with a color == "color"
		assertThat(colorCount).isEqualTo(recordsWithColourCounts.get(searchColor));
	}

	@Test
	public void testListBetweenQualifier() {
		long ageStart = ages[0]; // 25
		long ageEnd = ages[2]; // 27

		int age25Count = 0;
		int age26Count = 0;
		int age27Count = 0;
		String binName = "longList";

		Qualifier qualifier = new Qualifier(binName, FilterOperation.LIST_BETWEEN, Value.get(ageStart), Value.get(ageEnd));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				@SuppressWarnings("unchecked")
				List<Long> ageList = (List<Long>) rec.record.getList(binName);
				Long age = ageList.get(0);
				assertThat(age).isBetween(ageStart, ageEnd);
				if (age == 25) {
					age25Count++;
				} else if (age == 26) {
					age26Count++;
				} else {
					age27Count++;
				}
			}
		}

		assertThat(age25Count).isEqualTo(recordsWithAgeCounts.get(25));
		assertThat(age26Count).isEqualTo(recordsWithAgeCounts.get(26));
		assertThat(age27Count).isEqualTo(recordsWithAgeCounts.get(27));
	}

	@Test
	public void testMapKeysContainsQualifier() {
		String searchColor = colours[0];
		int colorCount = 0;

		String binName = "colorAgeMap";

		Qualifier qualifier = new Qualifier(binName, FilterOperation.MAP_KEYS_CONTAINS, Value.get(searchColor));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				@SuppressWarnings("unchecked")
				Map<String, ?> colorMap = (Map<String, ?>) rec.record.getMap(binName);
				assertThat(colorMap).containsKey(searchColor);
				colorCount++;
			}
		}

		// Every Record with a color == "color" has a one element map {"color" => #}
		// so there are an equal amount of records with the map {"color" => #} as with a color == "color"
		assertThat(colorCount).isEqualTo(recordsWithColourCounts.get(searchColor));
	}

	@Test
	public void testMapValuesContainsQualifier() {
		String searchColor = colours[0];
		int colorCount = 0;

		String binName = "ageColorMap";

		Qualifier qualifier = new Qualifier(binName, FilterOperation.MAP_VALUES_CONTAINS, Value.get(searchColor));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				@SuppressWarnings("unchecked")
				Map<?, String> colorMap = (Map<?, String>) rec.record.getMap(binName);
				assertThat(colorMap).containsValue(searchColor);
				colorCount++;
			}
		}

		// Every Record with a color == "color" has a one element map {"color" => #}
		// so there are an equal amount of records with the map {"color" => #} as with a color == "color"
		assertThat(colorCount).isEqualTo(recordsWithColourCounts.get(searchColor));
	}

	@Test
	public void testMapKeysBetweenQualifier() {
		long ageStart = ages[0]; // 25
		long ageEnd = ages[2]; // 27

		int age25Count = 0;
		int age26Count = 0;
		int age27Count = 0;
		String binName = "ageColorMap";

		Qualifier qualifier = new Qualifier(binName, FilterOperation.MAP_KEYS_BETWEEN, Value.get(ageStart), Value.get(ageEnd));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				@SuppressWarnings("unchecked")
				Map<Long, ?> ageColorMap = (Map<Long, ?>) rec.record.getMap(binName);
				// This is always a one item map
				for (Long age : ageColorMap.keySet()) {
					if (age == skipLongValue) {
						continue;
					}
					assertThat(age).isBetween(ageStart, ageEnd);
					if (age == 25) {
						age25Count++;
					} else if (age == 26) {
						age26Count++;
					} else {
						age27Count++;
					}
				}
			}
		}

		assertThat(age25Count).isEqualTo(recordsWithAgeCounts.get(25));
		assertThat(age26Count).isEqualTo(recordsWithAgeCounts.get(26));
		assertThat(age27Count).isEqualTo(recordsWithAgeCounts.get(27));
	}

	@Test
	public void testMapValuesBetweenQualifier() {
		long ageStart = ages[0]; // 25
		long ageEnd = ages[2]; // 27

		int age25Count = 0;
		int age26Count = 0;
		int age27Count = 0;
		String binName = "colorAgeMap";

		Qualifier qualifier = new Qualifier(binName, FilterOperation.MAP_VALUES_BETWEEN, Value.get(ageStart), Value.get(ageEnd));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				@SuppressWarnings("unchecked")
				Map<?, Long> colorAgeMap = (Map<?, Long>) rec.record.getMap(binName);
				// This is always a one item map
				for (Long age : colorAgeMap.values()) {
					if (age == skipLongValue) {
						continue;
					}
					assertThat(age).isBetween(ageStart, ageEnd);
					if (age == 25) {
						age25Count++;
					} else if (age == 26) {
						age26Count++;
					} else {
						age27Count++;
					}
				}
			}
		}

		assertThat(age25Count).isEqualTo(recordsWithAgeCounts.get(25));
		assertThat(age26Count).isEqualTo(recordsWithAgeCounts.get(26));
		assertThat(age27Count).isEqualTo(recordsWithAgeCounts.get(27));
	}

	@Test
	public void testContainingDoesNotUseSpecialCharacterQualifier() {
		int matchedCount = 0;
		Qualifier qualifier = new Qualifier(specialCharBin, FilterOperation.CONTAINING, Value.get(".*"));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				String scBin = it.next().record.getString(specialCharBin);
				assertThat(scBin).contains(".*");
				matchedCount++;
			}
		}
		assertThat(matchedCount).isEqualTo(3);
	}

	@Test
	public void testStartWithDoesNotUseSpecialCharacterQualifier() {
		int matchedCount = 0;
		Qualifier qualifier = new Qualifier(specialCharBin, FilterOperation.START_WITH, Value.get(".*"));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				String scBin = it.next().record.getString(specialCharBin);
				assertThat(scBin).startsWith(".*");
				matchedCount++;
			}
		}
		assertThat(matchedCount).isEqualTo(1);
	}

	@Test
	public void testEndWithDoesNotUseSpecialCharacterQualifier() {
		int matchedCount = 0;
		Qualifier qualifier = new Qualifier(specialCharBin, FilterOperation.ENDS_WITH, Value.get(".*"));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				String scBin = it.next().record.getString(specialCharBin);
				assertThat(scBin).endsWith(".*");
				matchedCount++;
			}
		}
		assertThat(matchedCount).isEqualTo(1);
	}

	@Test
	public void testEQIcaseDoesNotUseSpecialCharacter() {
		Qualifier qualifier = new Qualifier(specialCharBin, FilterOperation.EQ, true, Value.get(".*"));

		KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier);

		assertThat(it).toIterable().isEmpty();
	}

	@Test
	public void testContainingFindsSquareBracket() {
		int matchedCount = 0;

		String[] specialStrings = new String[]{"[", "$", "\\", "^"};
		for (String specialString : specialStrings) {
			matchedCount = 0;
			Qualifier qualifier = new Qualifier(specialCharBin, FilterOperation.CONTAINING, true, Value.get(specialString));
			try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
				while (it.hasNext()) {
					matchedCount++;
					String matchStr = it.next().record.getString(specialCharBin);
					assertThat(matchStr).contains(specialString);
				}
			}
			assertThat(matchedCount).isEqualTo(1);
		}
	}
}
