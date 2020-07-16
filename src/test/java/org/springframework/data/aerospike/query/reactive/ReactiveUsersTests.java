package org.springframework.data.aerospike.query.reactive;

import com.aerospike.client.Value;
import com.aerospike.client.query.KeyRecord;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.query.Qualifier;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.USERS_SET;

public class ReactiveUsersTests extends BaseReactiveQueryEngineTests {

	@Test
	public void usersInNorthRegion() {
		Qualifier qualifier = new Qualifier("region", Qualifier.FilterOperation.EQ, Value.get("n"));
		Flux<KeyRecord> flux = queryEngine.select(namespace, USERS_SET, null, qualifier);

		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					assertThat(results)
							.isNotEmpty()
							.allSatisfy(rec-> assertThat(rec.record.getString("region")).isEqualTo("n"));
					return true;
				})
				.verifyComplete();
	}

}
