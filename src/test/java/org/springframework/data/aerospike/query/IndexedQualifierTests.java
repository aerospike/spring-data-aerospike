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
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.CollectionUtils;

import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.CollectionUtils.countingInt;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation;

/*
 * These tests generate qualifiers on indexed bins.
 */
public class IndexedQualifierTests extends BaseQueryEngineTests {

	@Test
	public void selectOnIndexedLTQualifier() {
		withIndex(namespace, INDEXED_SET_NAME, "age_index", "age", IndexType.NUMERIC, () -> {
			// Ages range from 25 -> 29. We expected to only get back values with age < 26
			Qualifier qualifier = new Qualifier("age", FilterOperation.LT, Value.get(26));

			KeyRecordIterator iterator = queryEngine.select(namespace, INDEXED_SET_NAME, null, qualifier);

			assertThat(iterator)
					.toIterable()
					.isNotEmpty()
					.allSatisfy(rec -> assertThat(rec.record.getInt("age")).isLessThan(26))
					.hasSize(recordsWithAgeCounts.get(25));
			blockingAerospikeTestOperations.assertNoScansForSet(INDEXED_SET_NAME);
		});
	}

	@Test
	public void selectOnIndexedLTEQQualifier() {
		withIndex(namespace, INDEXED_SET_NAME, "age_index", "age", IndexType.NUMERIC, () -> {
			// Ages range from 25 -> 29. We expected to only get back values with age <= 26
			Qualifier qualifier = new Qualifier("age", FilterOperation.LTEQ, Value.get(26));

			KeyRecordIterator iterator = queryEngine.select(namespace, INDEXED_SET_NAME, null, qualifier);

			Map<Integer, Integer> ageCount = CollectionUtils.toStream(iterator)
					.map(rec -> rec.record.getInt("age"))
					.collect(Collectors.groupingBy(k -> k, countingInt()));
			assertThat(ageCount.keySet())
					.isNotEmpty()
					.allSatisfy(age -> assertThat(age).isLessThanOrEqualTo(26));
			assertThat(ageCount.get(25)).isEqualTo(recordsWithAgeCounts.get(25));
			assertThat(ageCount.get(26)).isEqualTo(recordsWithAgeCounts.get(26));
			blockingAerospikeTestOperations.assertNoScansForSet(INDEXED_SET_NAME);
		});
	}


	@Test
	public void selectOnIndexedNumericEQQualifier() {
		withIndex(namespace, INDEXED_SET_NAME, "age_index", "age", IndexType.NUMERIC, () -> {
			// Ages range from 25 -> 29. We expected to only get back values with age == 26
			Qualifier qualifier = new Qualifier("age", FilterOperation.EQ, Value.get(26));

			KeyRecordIterator iterator = queryEngine.select(namespace, INDEXED_SET_NAME, null, qualifier);

			assertThat(iterator)
					.toIterable()
					.isNotEmpty()
					.allSatisfy(rec -> assertThat(rec.record.getInt("age")).isEqualTo(26))
					.hasSize(recordsWithAgeCounts.get(26));
			blockingAerospikeTestOperations.assertNoScansForSet(INDEXED_SET_NAME);
		});
	}

	@Test
	public void selectOnIndexedGTEQQualifier() {
		withIndex(namespace, INDEXED_SET_NAME, "age_index", "age", IndexType.NUMERIC, () -> {
			// Ages range from 25 -> 29. We expected to only get back values with age >= 28
			Qualifier qualifier = new Qualifier("age", FilterOperation.GTEQ, Value.get(28));

			KeyRecordIterator iterator = queryEngine.select(namespace, INDEXED_SET_NAME, null, qualifier);

			Map<Integer, Integer> ageCount = CollectionUtils.toStream(iterator)
					.map(rec -> rec.record.getInt("age"))
					.collect(Collectors.groupingBy(k -> k, countingInt()));
			assertThat(ageCount.keySet())
					.isNotEmpty()
					.allSatisfy(age -> assertThat(age).isGreaterThanOrEqualTo(28));
			assertThat(ageCount.get(28)).isEqualTo(recordsWithAgeCounts.get(28));
			assertThat(ageCount.get(29)).isEqualTo(recordsWithAgeCounts.get(29));
			blockingAerospikeTestOperations.assertNoScansForSet(INDEXED_SET_NAME);
		});
	}

	@Test
	public void selectOnIndexedGTQualifier() {
		withIndex(namespace, INDEXED_SET_NAME, "age_index", "age", IndexType.NUMERIC, () -> {
			Qualifier qualifier = new Qualifier("age", FilterOperation.GT, Value.get(28));

			KeyRecordIterator iterator = queryEngine.select(namespace, INDEXED_SET_NAME, null, qualifier);

			assertThat(iterator)
					.toIterable()
					.isNotEmpty()
					.allSatisfy(rec -> assertThat(rec.record.getInt("age")).isEqualTo(29))
					.hasSize(recordsWithAgeCounts.get(29));
			blockingAerospikeTestOperations.assertNoScansForSet(INDEXED_SET_NAME);
		});
	}

	@Test
	public void selectOnIndexedStringEQQualifier() {
		withIndex(namespace, INDEXED_SET_NAME, "color_index", "color", IndexType.STRING, () -> {
			Qualifier qualifier = new Qualifier("color", FilterOperation.EQ, Value.get(ORANGE));

			KeyRecordIterator iterator = queryEngine.select(namespace, INDEXED_SET_NAME, null, qualifier);

			assertThat(iterator)
					.toIterable()
					.isNotEmpty()
					.allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(ORANGE))
					.hasSize(recordsWithColourCounts.get(ORANGE));
			blockingAerospikeTestOperations.assertNoScansForSet(INDEXED_SET_NAME);
		});
	}

	@Test
	public void selectWithGeoWithin() {
		withIndex(namespace, INDEXED_GEO_SET, "geo_index", GEO_BIN_NAME, IndexType.GEO2DSPHERE, () -> {
			double lon = -122.0;
			double lat = 37.5;
			double radius = 50000.0;
			String rgnstr = String.format("{ \"type\": \"AeroCircle\", "
							+ "\"coordinates\": [[%.8f, %.8f], %f] }",
					lon, lat, radius);
			Qualifier qualifier = new Qualifier(GEO_BIN_NAME, FilterOperation.GEO_WITHIN, Value.getAsGeoJSON(rgnstr));

			KeyRecordIterator iterator = queryEngine.select(namespace, INDEXED_GEO_SET, null, qualifier);

			assertThat(iterator).toIterable()
					.isNotEmpty()
					.allSatisfy(rec -> assertThat(rec.record.generation).isGreaterThanOrEqualTo(1));
			blockingAerospikeTestOperations.assertNoScansForSet(INDEXED_GEO_SET);
		});
	}

}
