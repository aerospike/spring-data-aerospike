package org.springframework.data.aerospike.repository.query.reactive.find;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.reactive.ReactiveCustomerRepositoryQueryTests;
import reactor.test.StepVerifier;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is not equal" reactive repository query. Keywords: Not, IsNot.
 */
public class NotEqualTests extends ReactiveCustomerRepositoryQueryTests {

    @Test
    public void findBySimplePropertyNotEqual_String() {
        StepVerifier.create(reactiveRepository.findByLastNameNot("Simpson"))
            .recordWith(List::of)
            .consumeRecordedWith(customers -> {
                assertThat(customers).containsExactlyInAnyOrderElementsOf(List.of(matt, leela, fry));
            })
            .expectComplete();

        StepVerifier.create(reactiveRepository.findByFirstNameNotIgnoreCase("SimpSon"))
            // this query returns Mono<Collection>
            .expectNextMatches(customers -> {
                assertThat(customers).containsExactlyInAnyOrderElementsOf(List.of(matt, leela, fry));
                return false;
            })
            .expectComplete();

        StepVerifier.create(reactiveRepository.findOneByLastNameNot("Simpson"))
            // this query returns Mono<Customer>
            .expectNextMatches(customer -> {
                assertThat(customer).isIn(List.of(matt, leela, fry));
                return false;
            })
            .expectComplete();
    }
}
