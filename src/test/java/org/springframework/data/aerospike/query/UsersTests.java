package org.springframework.data.aerospike.query;

import com.aerospike.client.Value;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.RECORD_COUNT;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.USERS_SET;

public class UsersTests extends BaseQueryEngineTests {

	@Test
	public void allUsers() {
		KeyRecordIterator it = queryEngine.select(namespace, USERS_SET, null);

		assertThat(it).toIterable().hasSize(RECORD_COUNT);
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
				.allSatisfy(rec -> assertThat(rec.record.getString("region")).isEqualTo("n"));
	}

}
