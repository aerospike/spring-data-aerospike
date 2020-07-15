package org.springframework.data.aerospike.query.reactive;

import com.aerospike.client.Value;
import com.aerospike.client.query.KeyRecord;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.query.KeyQualifier;
import org.springframework.data.aerospike.query.Qualifier;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;

import static junit.framework.TestCase.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.ENDS_WITH;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.EQ;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.GEO_WITHIN;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.START_WITH;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.COLOURS;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.COLOUR_COUNTS;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.GEO_BIN_NAME;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.GEO_SET;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.RECORD_COUNT;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.SET_NAME;

public class SelectorTests extends BaseReactiveQueryEngineTests {

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
		// Number of records containing a color ending with "e"
		long expectedEndsWithECount = Arrays.stream(COLOURS)
				.filter(c -> c.endsWith("e"))
				.mapToLong(c -> COLOUR_COUNTS.get(c))
				.sum();

		Qualifier qual1 = new Qualifier("color", ENDS_WITH, Value.get("e"));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, qual1);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					results.forEach(keyRecord ->
							Assert.assertTrue(keyRecord.record.getString("color").endsWith("e")));
					assertThat((long) results.size()).isEqualTo(expectedEndsWithECount);
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
					results.forEach(keyRecord ->
							Assert.assertTrue(keyRecord.record.getString("color").startsWith("bl")));
					assertThat(results.size()).isEqualTo(COLOUR_COUNTS.get("blue").intValue());
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
				.verifyComplete();
	}

	@Test
	public void startWithIgnoreCaseReturnsNoItemsIfNoneMatched() {
		boolean ignoreCase = false;
		Qualifier qual1 = new Qualifier("name", START_WITH, ignoreCase, Value.get("NA"));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, qual1);
		StepVerifier.create(flux)
				.verifyComplete();
	}

	@Test
	public void stringEqualIgnoreCaseWorksOnUnindexedBin() {
		boolean ignoreCase = true;
		String expectedColor = "blue";
		long blueRecordCount = 0;

		Qualifier caseInsensitiveQual = new Qualifier("color", EQ, ignoreCase, Value.get("BlUe"));
		Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, caseInsensitiveQual);
		StepVerifier.create(flux.collectList())
				.expectNextMatches(results -> {
					results.forEach(keyRecord ->
							assertThat(keyRecord.record.getString("color")).isEqualTo(expectedColor));
					assertThat(results.size()).isEqualTo(COLOUR_COUNTS.get("blue").intValue());
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
					results.forEach(keyRecord ->
							assertTrue(keyRecord.record.generation >= 1));
					return true;
				})
				.verifyComplete();
	}

}
