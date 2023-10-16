package org.springframework.data.aerospike.query.reactive;

import com.aerospike.client.Value;
import com.aerospike.client.query.KeyRecord;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.query.KeyQualifier;
import org.springframework.data.aerospike.query.Qualifier;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.query.FilterOperation.ENDS_WITH;
import static org.springframework.data.aerospike.query.FilterOperation.EQ;
import static org.springframework.data.aerospike.query.FilterOperation.GEO_WITHIN;
import static org.springframework.data.aerospike.query.FilterOperation.STARTS_WITH;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.BLUE;
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
        Qualifier qual1 = Qualifier.builder()
            .setField("color")
            .setFilterOperation(ENDS_WITH)
            .setValue1(Value.get("e"))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, qual1);
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                assertThat(results)
                    .allSatisfy(rec -> assertThat(rec.record.getString("color")).endsWith("e"))
                    .hasSize(queryEngineTestDataPopulator.colourCounts.get(ORANGE) + queryEngineTestDataPopulator.colourCounts.get(BLUE));
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void selectStartsWith() {
        Qualifier startsWithQual = Qualifier.builder()
            .setField("color")
            .setFilterOperation(STARTS_WITH)
            .setValue1(Value.get("bl"))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, startsWithQual);
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                assertThat(results)
                    .allSatisfy(rec -> assertThat(rec.record.getString("color")).startsWith("bl"))
                    .hasSize(queryEngineTestDataPopulator.colourCounts.get(BLUE));
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void startWithAndEqualIgnoreCaseReturnsAllItems() {
        boolean ignoreCase = true;
        Qualifier qual1 = Qualifier.builder()
            .setField("color")
            .setFilterOperation(EQ)
            .setIgnoreCase(ignoreCase)
            .setValue1(Value.get("BLUE"))
            .build();

        Qualifier qual2 = Qualifier.builder()
            .setField("name")
            .setFilterOperation(STARTS_WITH)
            .setIgnoreCase(ignoreCase)
            .setValue1(Value.get("NA"))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, qual1, qual2);
        StepVerifier.create(flux)
            .expectNextCount(queryEngineTestDataPopulator.colourCounts.get("blue"))
            .verifyComplete();
    }

    @Test
    public void equalIgnoreCaseReturnsNoItemsIfNoneMatched() {
        boolean ignoreCase = false;
        Qualifier qual1 = Qualifier.builder()
            .setField("color")
            .setFilterOperation(EQ)
            .setIgnoreCase(ignoreCase)
            .setValue1(Value.get("BLUE"))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, qual1);
        StepVerifier.create(flux)
            .expectNextCount(0)
            .verifyComplete();
    }

    @Test
    public void startWithIgnoreCaseReturnsNoItemsIfNoneMatched() {
        boolean ignoreCase = false;
        Qualifier qual1 = Qualifier.builder()
            .setField("name")
            .setFilterOperation(STARTS_WITH)
            .setIgnoreCase(ignoreCase)
            .setValue1(Value.get("NA"))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, qual1);
        StepVerifier.create(flux)
            .expectNextCount(0)
            .verifyComplete();
    }

    @Test
    public void stringEqualIgnoreCaseWorksOnUnindexedBin() {
        boolean ignoreCase = true;
        String expectedColor = "blue";

        Qualifier caseInsensitiveQual = Qualifier.builder()
            .setField("color")
            .setFilterOperation(EQ)
            .setIgnoreCase(ignoreCase)
            .setValue1(Value.get("BlUe"))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, SET_NAME, null, caseInsensitiveQual);
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                assertThat(results)
                    .allSatisfy(rec -> assertThat(rec.record.getString("color")).isEqualTo(expectedColor))
                    .hasSize(queryEngineTestDataPopulator.colourCounts.get(BLUE));
                return true;
            })
            .verifyComplete();
    }

    @Test
    public void stringEqualIgnoreCaseWorksRequiresFullMatch() {
        boolean ignoreCase = true;
        Qualifier caseInsensitiveQual = Qualifier.builder()
            .setField("color")
            .setFilterOperation(EQ)
            .setIgnoreCase(ignoreCase)
            .setValue1(Value.get("lue"))
            .build();

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
        Qualifier qual1 = Qualifier.builder()
            .setField(GEO_BIN_NAME)
            .setFilterOperation(GEO_WITHIN)
            .setValue1(Value.getAsGeoJSON(rgnstr))
            .build();

        Flux<KeyRecord> flux = queryEngine.select(namespace, GEO_SET, null, qual1);
        StepVerifier.create(flux.collectList())
            .expectNextMatches(results -> {
                assertThat(results)
                    .allSatisfy(rec -> assertThat(rec.record.generation).isPositive())
                    .isNotEmpty();
                return true;
            })
            .verifyComplete();
    }
}
