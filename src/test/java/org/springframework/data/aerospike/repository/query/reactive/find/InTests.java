package org.springframework.data.aerospike.repository.query.reactive.find;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.query.reactive.ReactiveCustomerRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Customer;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is in" reactive repository query. Keywords: In, IsIn.
 */
public class InTests extends ReactiveCustomerRepositoryQueryTests {

    @Test
    public void findByFirstnameIn_ShouldWorkProperly() {
        List<Customer> results = reactiveRepository.findByFirstNameIn(asList("Matt", "Homer"))
            .collectList().block();

        assertThat(results).containsOnly(homer, matt);
    }

    @Test
    public void findByIdAndFirstNameIn() {
        QueryParam ids = QueryParam.of(List.of(homer.getId(), marge.getId()));
        QueryParam firstNames = QueryParam.of(List.of(homer.getFirstName(), marge.getFirstName(), "FirstName"));
        StepVerifier.create(reactiveRepository.findByIdAndFirstNameIn(ids, firstNames).collectList())
            .expectNextMatches(list -> list.size() == 2 && list.contains(homer) && list.contains(marge))
            .verifyComplete();

        firstNames = QueryParam.of(List.of("FirstName"));
        StepVerifier.create(reactiveRepository.findByIdAndFirstNameIn(ids, firstNames))
            .expectComplete()
            .verify();
    }

    @Test
    public void findByIdAndFirstNameIn_Synonyms() {
        QueryParam ids = QueryParam.of(List.of(homer.getId(), marge.getId()));
        QueryParam firstNames = QueryParam.of(List.of(homer.getFirstName(), marge.getFirstName(), "FirstName"));
        StepVerifier.create(reactiveRepository.readByIdAndFirstNameIn(ids, firstNames).collectList())
            .expectNextMatches(list -> list.size() == 2 && list.contains(homer) && list.contains(marge))
            .verifyComplete();

        StepVerifier.create(reactiveRepository.readByIdAndFirstNameIn(ids, firstNames).collectList())
            .expectNextMatches(list -> list.size() == 2 && list.contains(homer) && list.contains(marge))
            .verifyComplete();

        StepVerifier.create(reactiveRepository.getByIdAndFirstNameIn(ids, firstNames).collectList())
            .expectNextMatches(list -> list.size() == 2 && list.contains(homer) && list.contains(marge))
            .verifyComplete();

        StepVerifier.create(reactiveRepository.queryByIdAndFirstNameIn(ids, firstNames).collectList())
            .expectNextMatches(list -> list.size() == 2 && list.contains(homer) && list.contains(marge))
            .verifyComplete();

        StepVerifier.create(reactiveRepository.searchByIdAndFirstNameIn(ids, firstNames).collectList())
            .expectNextMatches(list -> list.size() == 2 && list.contains(homer) && list.contains(marge))
            .verifyComplete();

        StepVerifier.create(reactiveRepository.streamByIdAndFirstNameIn(ids, firstNames).collectList())
            .expectNextMatches(list -> list.size() == 2 && list.contains(homer) && list.contains(marge))
            .verifyComplete();
    }
}
