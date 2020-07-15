package org.springframework.data.aerospike.query;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class UsersTests extends BaseQueryEngineTests {

	private static final String USERS_SET = "users";
	private static boolean usersCreated = false;

	@BeforeEach
	public void setUp() {
		if(usersCreated) return;
		createUsers();
		super.setUp();
		usersCreated = true;

	}

	@Test
	public void allUsers() {
		KeyRecordIterator it = queryEngine.select(namespace, USERS_SET, null);

		assertThat(it).toIterable().hasSize(BaseQueryEngineTests.RECORD_COUNT);
	}

	@Test
	public void usersInterupted() {
		try (KeyRecordIterator it = queryEngine.select(namespace, USERS_SET, null)) {
			int counter = 0;
			while (it.hasNext()) {
				it.next();
				counter++;
				if (counter >= 1000)
					break;
			}
		}
	}

	@Test
	public void usersInNorthRegion() {
		Qualifier qualifier = new Qualifier("region", Qualifier.FilterOperation.EQ, Value.get("n"));

		KeyRecordIterator it = queryEngine.select(namespace, USERS_SET, null, qualifier);

		assertThat(it).toIterable()
				.isNotEmpty()
				.allSatisfy(rec-> assertThat(rec.record.getString("region")).isEqualTo("n"));
	}

	public void createUsers() throws AerospikeException {
		String[] genders = {"m", "f"};
		String[] regions = {"n", "s", "e", "w"};
		String[] randomInterests = {"Music", "Football", "Soccer", "Baseball", "Basketball", "Hockey", "Weekend Warrior", "Hiking", "Camping", "Travel", "Photography"};
		String username;
		ArrayList<Object> userInterests = null;
		int totalInterests = 0;
		int start = 1;
		int end = BaseQueryEngineTests.RECORD_COUNT;
		/*
		 * see if data is loaded
		 */

		Key key = new Key(namespace, USERS_SET, "user" + (BaseQueryEngineTests.RECORD_COUNT - 99));
		if (!client.exists(null, key)) {
			Random rnd1 = new Random();
			Random rnd2 = new Random();
			Random rnd3 = new Random();


			for (int j = start; j <= end; j++) {
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
				userInterests = new ArrayList<Object>();
				for (int i = 0; i < totalInterests; i++) {
					userInterests.add(randomInterests[rnd3.nextInt(randomInterests.length)]);
				}
				Bin bin7 = new Bin("interests", userInterests);

				client.put(null, key, bin1, bin2, bin3, bin4, bin5, bin6, bin7);
			}
			createTweets();
		}
	}

	public void createTweets() throws AerospikeException {
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

}
