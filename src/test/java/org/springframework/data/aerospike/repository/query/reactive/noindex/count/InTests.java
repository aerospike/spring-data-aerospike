package org.springframework.data.aerospike.repository.query.reactive.noindex.count;

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
    public void countById_AND_SimplePropertyIn() {
        QueryParam ids = QueryParam.of(List.of(marge.getId(), homer.getId()));
        QueryParam firstNames = QueryParam.of(List.of(homer.getFirstName(), marge.getFirstName(), "FirstName"));
        StepVerifier.create(reactiveRepository.countByIdAndFirstNameIn(ids, firstNames))
            .expectNext(2L)
            .verifyComplete();

        firstNames = QueryParam.of(List.of("FirstName"));
        StepVerifier.create(reactiveRepository.countByIdAndFirstNameIn(ids, firstNames))
            .expectNext(0L)
            .verifyComplete();
    }
}
