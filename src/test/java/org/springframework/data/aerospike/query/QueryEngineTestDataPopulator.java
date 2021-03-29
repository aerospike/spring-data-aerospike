package org.springframework.data.aerospike.query;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class QueryEngineTestDataPopulator {

	public static final int RECORD_COUNT = 1000;
	private static final int SAMPLE_DATA_COUNT = 5;
	// These values are added to Lists and Maps to avoid single item collections.
	// Tests should ignore them for assertion purposes
	public static final long SKIP_LONG_VALUE = Long.MAX_VALUE;
	public static final String SKIP_COLOR_VALUE = "SKIP_THIS_COLOR";

	public static final String ORANGE = "orange";
	public static final String BLUE = "blue";
	public static final String GREEN = "green";

	public static final Integer[] AGES = new Integer[]{25, 26, 27, 28, 29};
	public static final String[] COLOURS = new String[]{BLUE, "red", "yellow", GREEN, ORANGE};
	public static final String[] ANIMALS = new String[]{"cat", "dog", "mouse", "snake", "lion"};

	public static final String SET_NAME = "selector";
	public static final String INDEXED_SET_NAME = "selector-indexed";

	public static final String GEO_SET = "geo-set";
	public static final String INDEXED_GEO_SET = "geo-set-indexed";
	public static final String GEO_BIN_NAME = "querygeobin";

	public static final String SPECIAL_CHAR_SET = "special-char-set";
	public static final String SPECIAL_CHAR_BIN = "scBin";
	public static final String keyPrefix = "querykey";

	public static final String USERS_SET = "users";

	public Map<Integer, Integer> ageCount = putZeroCountFor(AGES);
	public Map<String, Integer> colourCounts = putZeroCountFor(COLOURS);
	public Map<String, Integer> animalCounts = putZeroCountFor(ANIMALS);
	public Map<Long, Integer> modTenCounts = putZeroCountFor(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);

	private boolean dataPushed = false;

	private final String namespace;
	private final IAerospikeClient client;

	public void setupAllData() {
		if (dataPushed) return;

		setupData();
		setupGeoData();
		setupSpecialCharsData();
		setupUsers();
		dataPushed = true;
	}

	public void setupData() {
		int i = 0;
		for (int x = 1; x <= RECORD_COUNT; x++) {
			Map<Long, String> ageColorMap = new HashMap<>();
			ageColorMap.put((long) AGES[i], COLOURS[i]);
			ageColorMap.put(SKIP_LONG_VALUE, SKIP_COLOR_VALUE);

			Map<String, Long> colorAgeMap = new HashMap<>();
			colorAgeMap.put(COLOURS[i], (long) AGES[i]);
			colorAgeMap.put(SKIP_COLOR_VALUE, SKIP_LONG_VALUE);

			List<String> colorList = new ArrayList<>();
			colorList.add(COLOURS[i]);
			colorList.add(SKIP_COLOR_VALUE);

			List<Long> longList = new ArrayList<>();
			longList.add((long) AGES[i]);
			longList.add(SKIP_LONG_VALUE);

			Bin name = new Bin("name", "name:" + x);
			Bin age = new Bin("age", AGES[i]);
			Bin colour = new Bin("color", COLOURS[i]);
			Bin animal = new Bin("animal", ANIMALS[i]);
			Bin modTen = new Bin("modten", i % 10);

			Bin ageColorMapBin = new Bin("ageColorMap", ageColorMap);
			Bin colorAgeMapBin = new Bin("colorAgeMap", colorAgeMap);
			Bin colorListBin = new Bin("colorList", colorList);
			Bin longListBin = new Bin("longList", longList);

			Bin[] bins = new Bin[]{name, age, colour, animal, modTen, ageColorMapBin, colorAgeMapBin, colorListBin, longListBin};
			this.client.put(null, new Key(namespace, SET_NAME, "selector-test:" + x), bins);
			this.client.put(null, new Key(namespace, INDEXED_SET_NAME, "selector-test:" + x), bins);

			// Add to our counts of records written for each bin value
			ageCount.put(AGES[i], ageCount.get(AGES[i]) + 1);
			colourCounts.put(COLOURS[i], colourCounts.get(COLOURS[i]) + 1);
			animalCounts.put(ANIMALS[i], animalCounts.get(ANIMALS[i]) + 1);
			modTenCounts.put((long) (i % 10), modTenCounts.get((long) (i % 10)) + 1);

			i++;
			if (i == SAMPLE_DATA_COUNT)
				i = 0;
		}
	}

	private void setupGeoData() {
		for (int i = 0; i < RECORD_COUNT; i++) {
			double lng = -122 + (0.1 * i);
			double lat = 37.5 + (0.1 * i);
			if (isLatLngValidPair(lng, lat)) {
				Bin bin = Bin.asGeoJSON(GEO_BIN_NAME, buildGeoValue(lng, lat));
				client.put(null, new Key(namespace, GEO_SET, keyPrefix + i), bin);
				client.put(null, new Key(namespace, INDEXED_GEO_SET, keyPrefix + i), bin);
			}
		}
	}

	private boolean isLatLngValidPair(double lng, double lat) {
		return (-180 <= lng && lng <= 180
				&& -90 <= lat && lat <= 90);
	}

	private void setupSpecialCharsData() {
		Key ewsKey = new Key(namespace, SPECIAL_CHAR_SET, "ends-with-star");
		Bin ewsBin = new Bin(SPECIAL_CHAR_BIN, "abcd.*");
		this.client.put(null, ewsKey, ewsBin);

		Key swsKey = new Key(namespace, SPECIAL_CHAR_SET, "starts-with-star");
		Bin swsBin = new Bin(SPECIAL_CHAR_BIN, ".*abcd");
		this.client.put(null, swsKey, swsBin);

		Key starKey = new Key(namespace, SPECIAL_CHAR_SET, "mid-with-star");
		Bin starBin = new Bin(SPECIAL_CHAR_BIN, "a.*b");
		this.client.put(null, starKey, starBin);

		Key specialCharKey = new Key(namespace, SPECIAL_CHAR_SET, "special-chars");
		Bin specialCharsBin = new Bin(SPECIAL_CHAR_BIN, "a[$^\\ab");
		this.client.put(null, specialCharKey, specialCharsBin);
	}

	@SneakyThrows
	private void setupUsers() {
		String[] genders = {"m", "f"};
		String[] regions = {"n", "s", "e", "w"};
		String[] randomInterests = {"Music", "Football", "Soccer", "Baseball", "Basketball", "Hockey", "Weekend Warrior", "Hiking", "Camping", "Travel", "Photography"};
		String username;
		ArrayList<Object> userInterests;
		int totalInterests;
		int start = 1;
		/*
		 * see if data is loaded
		 */

		Key key = new Key(namespace, USERS_SET, "user" + (RECORD_COUNT - 99));
		if (!client.exists(null, key)) {
			Random rnd1 = new Random();
			Random rnd2 = new Random();
			Random rnd3 = new Random();


			for (int j = start; j <= RECORD_COUNT; j++) {
				// Write user record
				username = "user" + j;
				key = new Key(namespace, USERS_SET, username);
				Bin bin1 = new Bin("username", "user" + j);
				Bin bin2 = new Bin("password", "pwd" + j);
				Bin bin3 = new Bin("gender", genders[rnd1.nextInt(2)]);
				Bin bin4 = new Bin("region", regions[rnd2.nextInt(4)]);
				Bin bin5 = new Bin("lasttweeted", 0);
				Bin bin6 = new Bin("tweetcount", 0);

				totalInterests = rnd3.nextInt(7);
				userInterests = new ArrayList<>();
				for (int i = 0; i < totalInterests; i++) {
					userInterests.add(randomInterests[rnd3.nextInt(randomInterests.length)]);
				}
				Bin bin7 = new Bin("interests", userInterests);

				client.put(null, key, bin1, bin2, bin3, bin4, bin5, bin6, bin7);
			}
			createTweets();
		}
	}

	private void createTweets() throws AerospikeException {
		String[] randomTweets = {
				"For just $1 you get a half price download of half of the song and listen to it just once.",
				"People tell me my body looks like a melted candle",
				"Come on movie! Make it start!", "Byaaaayy",
				"Please, please, win! Meow, meow, meow!",
				"Put. A. Bird. On. It.",
				"A weekend wasted is a weekend well spent",
				"Would you like to super spike your meal?",
				"We have a mean no-no-bring-bag up here on aisle two.",
				"SEEK: See, Every, EVERY, Kind... of spot",
				"We can order that for you. It will take a year to get there.",
				"If you are pregnant, have a soda.",
				"Hear that snap? Hear that clap?",
				"Follow me and I may follow you",
				"Which is the best cafe in Portland? Discuss...",
				"Portland Coffee is for closers!",
				"Lets get this party started!",
				"How about them portland blazers!", "You got school'd, yo",
				"I love animals", "I love my dog", "What's up Portland",
				"Which is the best cafe in Portland? Discuss...",
				"I dont always tweet, but when I do it is on Tweetaspike"};
		Random rnd1 = new Random();
		Random rnd2 = new Random();
		Random rnd3 = new Random();
		Key userKey;
		Record userRecord;
		int totalUsers = 10000;
		int maxTweets = 20;
		String username;
		long ts = 0;


		for (int j = 0; j < totalUsers; j++) {
			// Check if user record exists
			username = "user" + rnd3.nextInt(100000);
			userKey = new Key(namespace, USERS_SET, username);
			userRecord = client.get(null, userKey);
			if (userRecord != null) {
				// create up to maxTweets random tweets for this user
				int totalTweets = rnd1.nextInt(maxTweets);
				for (int k = 1; k <= totalTweets; k++) {
					// Create timestamp to store along with the tweet so we can
					// query, index and report on it
					ts = getTimeStamp();
					Key tweetKey = new Key(namespace, "tweets", username + ":" + k);
					Bin bin1 = new Bin("tweet",
							randomTweets[rnd2.nextInt(randomTweets.length)]);
					Bin bin2 = new Bin("ts", ts);
					Bin bin3 = new Bin("username", username);

					client.put(null, tweetKey, bin1, bin2, bin3);
				}
				if (totalTweets > 0) {
					// Update tweet count and last tweet'd timestamp in the user
					// record
					client.put(null, userKey, new Bin("tweetcount", totalTweets), new Bin("lasttweeted", ts));
					//console.printf("\nINFO: The tweet count now is: " + totalTweets);
				}
			}
		}
	}

	private long getTimeStamp() {
		return System.currentTimeMillis();
	}

	private static String buildGeoValue(double lg, double lat) {
		return "{ \"type\": \"Point\", \"coordinates\": [" + lg + ", " + lat + "] }";
	}

	@SafeVarargs
	private static <K> Map<K, Integer> putZeroCountFor(K... keys) {
		return Arrays.stream(keys).collect(Collectors.toMap(k -> k, k -> 0));
	}
}
