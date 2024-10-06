package org.springframework.data.aerospike.repository.query.reactive.noindex.countBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.reactive.noindex.ReactiveCustomerRepositoryQueryTests;
import reactor.test.StepVerifier;

/**
 * Tests for the "Is between" reactive repository query. Keywords: Between, IsBetween.
 */
public class BetweenTests extends ReactiveCustomerRepositoryQueryTests {

    @Test
    public void countBySimplePropertyBetween_Integer() {
        StepVerifier.create(reactiveRepository.countByAgeBetween(20, 1100))
            .expectNextMatches(result -> result >= 2)
            .verifyComplete();
    }
}
