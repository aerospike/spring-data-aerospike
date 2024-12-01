package org.springframework.data.aerospike.repository.query.reactive.exists;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.query.reactive.ReactiveCustomerRepositoryQueryTests;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

/**
 * Tests for the "Equals" reactive repository query. Keywords: Is, Equals (or no keyword).
 */
public class EqualsTests extends ReactiveCustomerRepositoryQueryTests {

    @Test
    public void existsById_ShouldReturnTrueWhenExists() {
        StepVerifier.create(reactiveRepository.existsById(leela.getId()))
            .expectNext(true).verifyComplete();
    }

    @Test
    public void existsById_ShouldReturnFalseWhenNotExists() {
        StepVerifier.create(reactiveRepository.existsById("non-existent-id"))
            .expectNext(false)
            .verifyComplete();
    }

    @Test
    public void existsByIdPublisher_ShouldReturnTrueWhenExists() {
        StepVerifier.create(reactiveRepository.existsById(Flux.just(fry.getId())))
            .expectNext(true).verifyComplete();
    }

    @Test
    public void existsByIdPublisher_ShouldReturnFalseWhenNotExists() {
        StepVerifier.create(reactiveRepository.existsById(Flux.just("non-existent-id")))
            .expectNext(false).verifyComplete();
    }

    @Test
    public void existsByIdPublisher_ShouldCheckOnlyFirstElement() {
        StepVerifier.create(reactiveRepository.existsById(Flux.just(fry.getId(), "non-existent-id")))
            .expectNext(true).verifyComplete();
    }

    @Test
    public void existsById_AND_SimpleProperty() {
        QueryParam id = QueryParam.of(leela.getId());
        QueryParam firstname = QueryParam.of(leela.getFirstName());
        StepVerifier.create(reactiveRepository.existsByIdAndFirstName(id, firstname))
            .expectNext(true)
            .verifyComplete();

        firstname = QueryParam.of("FirstName");
        StepVerifier.create(reactiveRepository.existsByIdAndFirstName(id, firstname))
            .expectNext(false)
            .verifyComplete();
    }
}
