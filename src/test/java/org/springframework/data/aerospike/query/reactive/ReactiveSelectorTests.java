package org.springframework.data.aerospike.query.reactive;

import com.aerospike.client.Value;
import com.aerospike.client.query.KeyRecord;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.query.KeyQualifier;
import org.springframework.data.aerospike.query.Qualifier;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.ENDS_WITH;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.EQ;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.GEO_WITHIN;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.START_WITH;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.BLUE;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.COLOUR_COUNTS;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.GEO_BIN_NAME;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.GEO_SET;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.ORANGE;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.RECORD_COUNT;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.SET_NAME;

public class ReactiveSelectorTests extends BaseReactiveQueryEngineTests {

	@Test
	public void selectOneWithKey() {
		KeyQualifier kq = new KeyQualifier(Value.get("selector-test:3"));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, kq);

		StepVerifier.create(flux)
				.expectNextCount(1)
				.verifyComplete();
	}

	@Test
	public void selectOneWithNonExistingKey() {
		KeyQualifier kq = new KeyQualifier(Value.get("selector-test:no-such-record"));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, kq);

		StepVerifier.create(flux)
				.expectNextCount(0)
				.verifyComplete();
	}

	@Test
	public void selectAll() {
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null);

		StepVerifier.create(flux)
				.expectNextCount(RECORD_COUNT)
				.verifyComplete();
	}

	@Test
	public void selectEndssWith() {
		Qualifier qual1 = new Qualifier("color", ENDS_WITH, Value.get("e"));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, qual1);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					assertThat(results)
							.allSatisfy(rec -> assertThat(rec.record.getString("color")).endsWith("e"))
							.hasSize(COLOUR_COUNTS.get(ORANGE) + COLOUR_COUNTS.get(BLUE));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void selectStartsWith() {
		Qualifier startsWithQual = new Qualifier("color", START_WITH, Value.get("bl"));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, startsWithQual);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					assertThat(results)
							.allSatisfy(rec -> assertThat(rec.record.getString("color")).startsWith("bl"))
							.hasSize(COLOUR_COUNTS.get(BLUE));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void startWithAndEqualIgnoreCaseReturnsAllItems() {
		boolean ignoreCase = true;
		Qualifier qual1 = new Qualifier("color", EQ, ignoreCase, Value.get("BLUE"));
		Qualifier qual2 = new Qualifier("name", START_WITH, ignoreCase, Value.get("NA"));

		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, qual1, qual2);
		StepVerifier.create(flux)
				.expectNextCount(COLOUR_COUNTS.get("blue"))
				.verifyComplete();
	}

	@Test
	public void equalIgnoreCaseReturnsNoItemsIfNoneMatched() {
		boolean ignoreCase = false;
		Qualifier qual1 = new Qualifier("color", EQ, ignoreCase, Value.get("BLUE"));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, qual1);
		StepVerifier.create(flux)
				.expectNextCount(0)
				.verifyComplete();
	}

	@Test
	public void startWithIgnoreCaseReturnsNoItemsIfNoneMatched() {
		boolean ignoreCase = false;
		Qualifier qual1 = new Qualifier("name", START_WITH, ignoreCase, Value.get("NA"));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, qual1);
		StepVerifier.create(flux)
				.expectNextCount(0)
				.verifyComplete();
	}

	@Test
	public void stringEqualIgnoreCaseWorksOnUnindexedBin() {
		boolean ignoreCase = true;
		String expectedColor = "blue";

		Qualifier caseInsensitiveQual = new Qualifier("color", EQ, ignoreCase, Value.get("BlUe"));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, caseInsensitiveQual);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					assertThat(results)
							.allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(expectedColor))
							.hasSize(COLOUR_COUNTS.get(BLUE));
					return true;
				})
				.verifyComplete();
	}

	@Test
	public void stringEqualIgnoreCaseWorksRequiresFullMatch() {
		boolean ignoreCase = true;
		Qualifier caseInsensitiveQual = new Qualifier("color", EQ, ignoreCase, Value.get("lue"));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, caseInsensitiveQual);

		StepVerifier.create(flux)
				.expectNextCount(0)
				.verifyComplete();
	}

	@Test
	public void selectWithGeoWithin() {
		double lon = -122.0;
		double lat = 37.5;
		double radius = 50000.0;
		String rgnstr = String.format("{ \"type\": \"AeroCircle\", "
						+ "\"coordinates\": [[%.8f, %.8f], %f] }",
				lon, lat, radius);
		Qualifier qual1 = new Qualifier(GEO_BIN_NAME, GEO_WITHIN, Value.getAsGeoJSON(rgnstr));
		Flux<KeyRecord> flux = queryEngine.select(namespace, GEO_SET, null, qual1);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					assertThat(results)
							.allSatisfy(rec -> assertThat(rec.record.generation).isGreaterThanOrEqualTo(1))
							.isNotEmpty();
					return true;
				})
				.verifyComplete();
	}

}
