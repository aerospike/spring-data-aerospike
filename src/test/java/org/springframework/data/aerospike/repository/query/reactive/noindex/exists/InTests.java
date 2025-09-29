package org.springframework.data.aerospike.repository.query.reactive.exists;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.query.reactive.noindex.ReactiveCustomerRepositoryQueryTests;
import reactor.test.StepVerifier;

import java.util.List;

/**
 * Tests for the "Is in" reactive repository query. Keywords: In, IsIn.
 */
public class InTests extends ReactiveCustomerRepositoryQueryTests {

    @Test
    public void existsById_AND_SImplePropertyIn() {
        QueryParam ids = QueryParam.of(List.of(fry.getId(), leela.getId()));
        QueryParam firstNames = QueryParam.of(List.of(fry.getFirstName(), leela.getFirstName(), "FirstName"));
        StepVerifier.create(reactiveRepository.existsByIdAndFirstNameIn(ids, firstNames))
            .expectNext(true)
            .verifyComplete();

        firstNames = QueryParam.of(List.of("FirstName"));
        StepVerifier.create(reactiveRepository.existsByIdAndFirstNameIn(ids, firstNames))
            .expectNext(false)
            .verifyComplete();
    }
}
