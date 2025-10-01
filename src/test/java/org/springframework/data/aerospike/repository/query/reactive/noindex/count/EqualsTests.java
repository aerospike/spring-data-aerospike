package org.springframework.data.aerospike.repository.query.reactive.noindex.count;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.query.reactive.noindex.ReactiveCustomerRepositoryQueryTests;
import reactor.test.StepVerifier;

/**
 * Tests for the "Equals" reactive repository query. Keywords: Is, Equals (or no keyword).
 */
public class EqualsTests extends ReactiveCustomerRepositoryQueryTests {

    @Test
    public void countById_AND_SimpleProperty() {
        QueryParam id = QueryParam.of(marge.getId());
        QueryParam firstName = QueryParam.of(marge.getFirstName());
        StepVerifier.create(reactiveRepository.countByIdAndFirstName(id, firstName))
            .expectNext(1L)
            .verifyComplete();

        firstName = QueryParam.of(marge.getFirstName() + "_");
        StepVerifier.create(reactiveRepository.countByIdAndFirstName(id, firstName))
            .expectNext(0L)
            .verifyComplete();
    }
}
