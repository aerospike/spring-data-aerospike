package org.springframework.data.aerospike.query;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.IndexUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class BaseQueryEngineTests extends BaseBlockingIntegrationTests {

	public static final String SET_NAME = "selector";
	public static final String INDEXED_SET_NAME = "selector-indexed";
	public static final int RECORD_COUNT = 1000;
	private static final int SAMPLE_DATA_COUNT = 5;
	// These values are added to Lists and Maps to avoid single item collections.
	// Tests should ignore them for assertion purposes
	protected static final long skipLongValue = Long.MAX_VALUE;
	protected static final String skipColorValue = "SKIP_THIS_COLOR";

	public static final String ORANGE = "orange";
	public static final String BLUE = "blue";
	public static final String GREEN = "green";

	protected static final Integer[] ages = new Integer[]{25, 26, 27, 28, 29};
	protected static final String[] colours = new String[]{BLUE, "red", "yellow", GREEN, ORANGE};
	protected static final String[] animals = new String[]{"cat", "dog", "mouse", "snake", "lion"};

	protected static final String GEO_SET = "geo-set";
	protected static final String INDEXED_GEO_SET = "geo-set-indexed";
	protected static final String GEO_BIN_NAME = "querygeobin";

	protected static final String SPECIAL_CHAR_SET = "special-char-set";
	protected static final String specialCharBin = "scBin";
	private static final String keyPrefix = "querykey";

	private static boolean dataPushed = false;

	protected static Map<Integer, Integer> recordsWithAgeCounts;
	protected static Map<String, Integer> recordsWithColourCounts;
	protected static Map<String, Integer> recordsWithAnimalCounts;
	protected static Map<Long, Integer> recordsModTenCounts;

	@BeforeEach
	public void setUp() {
		if (dataPushed) return;

		setupData();
		setupGeoData();
		setupSpecialCharsData();

		dataPushed = true;
	}

	private void setupData() {
		initializeMaps();
		int i = 0;
		for (int x = 1; x <= RECORD_COUNT; x++) {
			Map<Long, String> ageColorMap = new HashMap<>();
			ageColorMap.put((long) ages[i], colours[i]);
			ageColorMap.put(skipLongValue, skipColorValue);

			Map<String, Long> colorAgeMap = new HashMap<>();
			colorAgeMap.put(colours[i], (long) ages[i]);
			colorAgeMap.put(skipColorValue, skipLongValue);

			List<String> colorList = new ArrayList<>();
			colorList.add(colours[i]);
			colorList.add(skipColorValue);

			List<Long> longList = new ArrayList<>();
			longList.add((long) ages[i]);
			longList.add(skipLongValue);

			Bin name = new Bin("name", "name:" + x);
			Bin age = new Bin("age", ages[i]);
			Bin colour = new Bin("color", colours[i]);
			Bin animal = new Bin("animal", animals[i]);
			Bin modTen = new Bin("modten", i % 10);

			Bin ageColorMapBin = new Bin("ageColorMap", ageColorMap);
			Bin colorAgeMapBin = new Bin("colorAgeMap", colorAgeMap);
			Bin colorListBin = new Bin("colorList", colorList);
			Bin longListBin = new Bin("longList", longList);

			Bin[] bins = new Bin[]{name, age, colour, animal, modTen, ageColorMapBin, colorAgeMapBin, colorListBin, longListBin};
//			WritePolicy policy = new WritePolicy(client.writePolicyDefault);
			client.writePolicyDefault.sendKey = true;
			this.client.put(client.writePolicyDefault, new Key(namespace, SET_NAME, "selector-test:" + x), bins);
			this.client.put(null, new Key(namespace, INDEXED_SET_NAME, "selector-test:" + x), bins);

			// Add to our counts of records written for each bin value
			recordsWithAgeCounts.put(ages[i], recordsWithAgeCounts.get(ages[i]) + 1);
			recordsWithColourCounts.put(colours[i], recordsWithColourCounts.get(colours[i]) + 1);
			recordsWithAnimalCounts.put(animals[i], recordsWithAnimalCounts.get(animals[i]) + 1);
			recordsModTenCounts.put((long) (i % 10), recordsModTenCounts.get((long) (i % 10)) + 1);

			i++;
			if (i == SAMPLE_DATA_COUNT)
				i = 0;
		}
	}

	private void initializeMaps() {
		recordsWithAgeCounts = putZeroCountFor(ages);
		recordsWithColourCounts = putZeroCountFor(colours);
		recordsWithAnimalCounts = putZeroCountFor(animals);
		recordsModTenCounts = putZeroCountFor(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
	}

	private void setupGeoData() {
		for (int i = 0; i < RECORD_COUNT; i++) {
			double lng = -122 + (0.1 * i);
			double lat = 37.5 + (0.1 * i);
			Bin bin = Bin.asGeoJSON(GEO_BIN_NAME, buildGeoValue(lng, lat));
			client.put(null, new Key(namespace, GEO_SET, keyPrefix + i), bin);
			client.put(null, new Key(namespace, INDEXED_GEO_SET, keyPrefix + i), bin);
		}
	}

	private void setupSpecialCharsData() {
		Key ewsKey = new Key(namespace, SPECIAL_CHAR_SET, "ends-with-star");
		Bin ewsBin = new Bin(specialCharBin, "abcd.*");
		this.client.put(null, ewsKey, ewsBin);

		Key swsKey = new Key(namespace, SPECIAL_CHAR_SET, "starts-with-star");
		Bin swsBin = new Bin(specialCharBin, ".*abcd");
		this.client.put(null, swsKey, swsBin);

		Key starKey = new Key(namespace, SPECIAL_CHAR_SET, "mid-with-star");
		Bin starBin = new Bin(specialCharBin, "a.*b");
		this.client.put(null, starKey, starBin);

		Key specialCharKey = new Key(namespace, SPECIAL_CHAR_SET, "special-chars");
		Bin specialCharsBin = new Bin(specialCharBin, "a[$^\\ab");
		this.client.put(null, specialCharKey, specialCharsBin);
	}

	private static String buildGeoValue(double lg, double lat) {
		return "{ \"type\": \"Point\", \"coordinates\": [" + lg + ", " + lat + "] }";
	}

	@SafeVarargs
	private static <K> Map<K, Integer> putZeroCountFor(K... keys) {
		return Arrays.stream(keys).collect(Collectors.toMap(k -> k, k -> 0));
	}

	protected void withIndex(String namespace, String setName, String indexName, String binName, IndexType indexType, Runnable runnable) {
		tryCreateIndex(namespace, setName, indexName, binName, indexType);
		try {
			runnable.run();
		} finally {
			tryDropIndex(namespace, setName, indexName);
		}
	}

	protected void tryDropIndex(String namespace, String setName, String indexName) {
		IndexUtils.dropIndex(client, namespace, setName, indexName);
		indexRefresher.refreshIndexes();
	}

	protected void tryCreateIndex(String namespace, String setName, String indexName, String binName, IndexType indexType) {
		IndexUtils.createIndex(client, namespace, setName, indexName, binName, indexType);
		indexRefresher.refreshIndexes();
	}

	protected void tryCreateIndex(String namespace, String setName, String indexName, String binName, IndexType indexType,
								  IndexCollectionType collectionType) {
		IndexUtils.createIndex(client, namespace, setName, indexName, binName, indexType, collectionType);
		indexRefresher.refreshIndexes();
	}

}
