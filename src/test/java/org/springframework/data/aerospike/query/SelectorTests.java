package org.springframework.data.aerospike.query;

import com.aerospike.client.Value;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.CollectionUtils;
import org.springframework.data.aerospike.query.Qualifier.FilterOperation;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

//TODO: review these tests, probably some of them are already duplicates of IndexedQualifierTests or QualifierTests
public class SelectorTests extends BaseQueryEngineTests {

	@Test
	public void selectOneWitKey() {
		Statement stmt = new Statement();
		stmt.setNamespace(namespace);
		stmt.setSetName(BaseQueryEngineTests.SET_NAME);
		KeyQualifier kq = new KeyQualifier(Value.get("selector-test:3"));

		KeyRecordIterator iterator = queryEngine.select(stmt, kq);

		assertThat(iterator).toIterable().hasSize(1);
	}

	@Test
	public void selectAll() {
		KeyRecordIterator iterator = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null);

		assertThat(iterator).toIterable().hasSize(BaseQueryEngineTests.RECORD_COUNT);
	}

	@Test
	public void selectOnIndexFilter() {
		withIndex(namespace, BaseQueryEngineTests.SET_NAME, "age_index", "age", IndexType.NUMERIC, () -> {
			int age28Count = 0;
			int age29Count = 0;
			Filter filter = Filter.range("age", 28, 29);
			try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, filter)) {
				while (it.hasNext()) {
					KeyRecord rec = it.next();
					int age = rec.record.getInt("age");
					assertThat(age).isBetween(28, 29);
					if (age == 28) {
						age28Count++;
					}
					if (age == 29) {
						age29Count++;
					}
				}
			}
			assertThat(age28Count).isEqualTo(recordsWithAgeCounts.get(28));
			assertThat(age29Count).isEqualTo(recordsWithAgeCounts.get(29));
		});
	}

	/*
	 * If we use a qualifier on an indexed bin, The QueryEngine will generate a Filter. Verify that a LTEQ Filter Operation
	 * Generates the correct Filter.Range() filter.
	 */
	@Test
	public void selectOnIndexedLTEQQualifier() {
		withIndex(namespace, BaseQueryEngineTests.SET_NAME, "age_index", "age", IndexType.NUMERIC, () -> {
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
		});


	}

	@Test
	public void selectEndssWith() {
		Qualifier qual1 = new Qualifier("color", FilterOperation.ENDS_WITH, Value.get("e"));
		int endsWithECount = 0;
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qual1)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				assertThat(rec.record.getString("color")).endsWith("e");
				endsWithECount++;
			}
		}
		// Number of records containing a color ending with "e"
		long expectedEndsWithECount = Arrays.stream(colours)
				.filter(c -> c.endsWith("e"))
				.mapToLong(c -> recordsWithColourCounts.get(c))
				.sum();

		assertThat(endsWithECount).isEqualTo(expectedEndsWithECount);

	}

	@Test
	public void selectStartsWith() {
		Qualifier startsWithQual = new Qualifier("color", FilterOperation.START_WITH, Value.get("bl"));
		int startsWithBL = 0;
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, startsWithQual)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				assertThat(rec.record.getString("color")).startsWith("bl");
				startsWithBL++;
			}
		}
		assertThat(startsWithBL).isEqualTo(recordsWithColourCounts.get("blue"));
	}

	@Test
	public void startWithAndEqualIgnoreCaseReturnsAllItems() {
		boolean ignoreCase = true;
		Qualifier qual1 = new Qualifier("color", FilterOperation.EQ, ignoreCase, Value.get("BLUE"));
		Qualifier qual2 = new Qualifier("name", FilterOperation.START_WITH, ignoreCase, Value.get("NA"));

		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qual1, qual2)) {
			List<KeyRecord> result = CollectionUtils.toList(it);

			assertThat(result).hasSize(recordsWithColourCounts.get("blue"));
		}
	}

	@Test
	public void equalIgnoreCaseReturnsNoItemsIfNoneMatched() {
		boolean ignoreCase = false;
		Qualifier qual1 = new Qualifier("color", FilterOperation.EQ, ignoreCase, Value.get("BLUE"));

		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qual1)) {
			List<KeyRecord> result = CollectionUtils.toList(it);

			assertThat(result).isEmpty();
		}
	}

	@Test
	public void startWithIgnoreCaseReturnsNoItemsIfNoneMatched() {
		boolean ignoreCase = false;
		Qualifier qual1 = new Qualifier("name", FilterOperation.START_WITH, ignoreCase, Value.get("NA"));

		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qual1)) {
			List<KeyRecord> result = CollectionUtils.toList(it);

			assertThat(result).isEmpty();
		}
	}

	@Test
	public void stringEqualIgnoreCaseWorksOnIndexedBin() {


		withIndex(namespace, BaseQueryEngineTests.SET_NAME, "color_index_selector", "color", IndexType.STRING, () -> {
			boolean ignoreCase = true;
			String expectedColor = "blue";
			int blueRecordCount = 0;
			Qualifier qualifier = new Qualifier("color", FilterOperation.EQ, ignoreCase, Value.get("BlUe"));
			try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
				while (it.hasNext()) {
					KeyRecord kr = it.next();
					String color = kr.record.getString("color");
					assertThat(color).isEqualTo(expectedColor);
					blueRecordCount++;
				}
			}
			assertThat(blueRecordCount).isEqualTo(recordsWithColourCounts.get("blue"));

		});

	}

	@Test
	public void stringEqualIgnoreCaseWorksOnUnindexedBin() {
		boolean ignoreCase = true;
		int blueRecordCount = 0;

		Qualifier qualifier = new Qualifier("color", FilterOperation.EQ, ignoreCase, Value.get("BlUe"));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			while (it.hasNext()) {
				KeyRecord kr = it.next();
				String color = kr.record.getString("color");
				assertThat(color).isEqualTo(BLUE);
				blueRecordCount++;
			}
		}
		assertThat(blueRecordCount).isEqualTo(recordsWithColourCounts.get(BLUE));
	}

	@Test
	public void stringEqualIgnoreCaseWorksRequiresFullMatch() {
		boolean ignoreCase = true;
		Qualifier qualifier = new Qualifier("color", FilterOperation.EQ, ignoreCase, Value.get("lue"));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qualifier)) {
			List<KeyRecord> result = CollectionUtils.toList(it);
			assertThat(result.size()).isEqualTo(0);
		}

	}

	@Test
	public void selectOnIndexWithQualifiers() {
		withIndex(namespace, BaseQueryEngineTests.SET_NAME, "age_index_selector", "age", IndexType.NUMERIC, () -> {
			Filter filter = Filter.range("age", 25, 29);
			Qualifier qual1 = new Qualifier("color", FilterOperation.EQ, Value.get(BLUE));
			int blueCount = 0;
			try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, filter, qual1)) {
				while (it.hasNext()) {
					KeyRecord rec = it.next();
					assertThat(rec.record.getString("color")).isEqualTo(BLUE);
					blueCount++;
					int age = rec.record.getInt("age");
					assertThat(age).isBetween(25, 29);
				}
			}
			assertThat(blueCount).isEqualTo(recordsWithColourCounts.get(BLUE));
		});


	}

	@Test
	public void selectWithQualifiersOnly() {
		withIndex(namespace, BaseQueryEngineTests.SET_NAME, "age_index", "age", IndexType.NUMERIC, () -> {
			Qualifier qual1 = new Qualifier("color", FilterOperation.EQ, Value.get(GREEN));
			Qualifier qual2 = new Qualifier("age", FilterOperation.BETWEEN, Value.get(28), Value.get(29));
			try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qual1, qual2)) {
				while (it.hasNext()) {
					KeyRecord rec = it.next();
					assertThat(rec.record.getString("color")).isEqualTo(GREEN);
					assertThat(rec.record.getInt("age")).isBetween(28, 29);
				}
			}
		});
	}

	@Test
	public void selectWithOrQualifiers() {
		withIndex(namespace, BaseQueryEngineTests.SET_NAME, "age_index", "age", IndexType.NUMERIC, () -> {
			String expectedColor = colours[0];
			int colorMatched = 0;
			int ageMatched = 0;

			// We are  expecting to get back all records where color == blue or (age == 28 || age == 29)
			Qualifier qual1 = new Qualifier("color", FilterOperation.EQ, Value.get(expectedColor));
			Qualifier qual2 = new Qualifier("age", FilterOperation.BETWEEN, Value.get(28), Value.get(29));
			Qualifier or = new Qualifier(FilterOperation.OR, qual1, qual2);
			try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, or)) {
				while (it.hasNext()) {
					KeyRecord rec = it.next();
					int age = rec.record.getInt("age");
					String color = rec.record.getString("color");

					Assert.assertTrue(expectedColor.equals(color) || (age >= 28 && age <= 29));
					if (expectedColor.equals(color)) {
						colorMatched++;
					}
					if ((age >= 28 && age <= 29)) {
						ageMatched++;
					}
				}
			}
			assertThat(colorMatched).isEqualTo(recordsWithColourCounts.get(expectedColor));
			assertThat(ageMatched).isEqualTo(recordsWithAgeCounts.get(28) + recordsWithAgeCounts.get(29));
		});
	}

	@Test
	public void selectWithBetweenAndOrQualifiers() {
		withIndex(namespace, BaseQueryEngineTests.SET_NAME, "age_index", "age", IndexType.NUMERIC, () -> {
			Qualifier qual1 = new Qualifier("color", FilterOperation.EQ, Value.get(GREEN));
			Qualifier qual2 = new Qualifier("age", FilterOperation.BETWEEN, Value.get(28), Value.get(29));
			Qualifier qual3 = new Qualifier("age", FilterOperation.EQ, Value.get(25));
			Qualifier qual4 = new Qualifier("name", FilterOperation.EQ, Value.get("name:696"));
			Qualifier or = new Qualifier(FilterOperation.OR, qual3, qual2, qual4);
			Qualifier or2 = new Qualifier(FilterOperation.OR, qual1, qual4);
			Qualifier and = new Qualifier(FilterOperation.AND, or, or2);

			try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, and)) {
				boolean has25 = false;
				while (it.hasNext()) {
					KeyRecord rec = it.next();
					int age = rec.record.getInt("age");
					if (age == 25) {
						has25 = true;
					} else {
						assertThat(rec.record.getString("color")).isEqualTo(GREEN);
						Assert.assertTrue(age == 25 || (age >= 28 && age <= 29));
					}
				}
				assertThat(has25).isTrue();
			}
		});
	}

	@Test
	public void selectWithGeoWithin() {
		double lon = -122.0;
		double lat = 37.5;
		double radius = 50000.0;
		String rgnstr = String.format("{ \"type\": \"AeroCircle\", "
						+ "\"coordinates\": [[%.8f, %.8f], %f] }",
				lon, lat, radius);
		Statement stmt = new Statement();
		stmt.setNamespace(namespace);
		stmt.setSetName(GEO_SET);
		Qualifier qual1 = new Qualifier(GEO_SET, FilterOperation.GEO_WITHIN, Value.getAsGeoJSON(rgnstr));
		try (KeyRecordIterator it = queryEngine.select(namespace, BaseQueryEngineTests.SET_NAME, null, qual1)) {
			while (it.hasNext()) {
				KeyRecord rec = it.next();
				assertThat(rec.record.generation).isGreaterThanOrEqualTo(1);
			}
		}
	}

}
