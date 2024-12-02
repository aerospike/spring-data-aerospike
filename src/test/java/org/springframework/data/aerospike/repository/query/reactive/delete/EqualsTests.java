package org.springframework.data.aerospike.repository.query.reactive.delete;

import com.aerospike.client.AerospikeException;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.query.reactive.ReactiveCustomerRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Customer;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/**
 * Tests for the "Equals" reactive repository query. Keywords: Is, Equals (or no keyword).
 */
public class EqualsTests extends ReactiveCustomerRepositoryQueryTests {

    @Test
    public void deleteById_ShouldDeleteExistent() {
        StepVerifier.create(reactiveRepository.deleteById(marge.getId()))
            .verifyComplete();

        StepVerifier.create(reactiveRepository.findById(marge.getId())).expectNextCount(0).verifyComplete();

        // cleanup
        StepVerifier.create(reactiveRepository.save(marge))
            .expectNextCount(1)
            .verifyComplete();
    }

    @Test
    public void deleteById_ShouldSkipNonexistent() {
        StepVerifier.create(reactiveRepository.deleteById("non-existent-id"))
            .verifyComplete();
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void deleteById_ShouldRejectNullObject() {
        assertThatThrownBy(() -> reactiveRepository.deleteById((String) null).block())
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void deleteByIdPublisher_ShouldDeleteOnlyFirstElement() {
        StepVerifier.create(
            reactiveRepository
                .deleteById(Flux.just(homer.getId(), marge.getId())))
            .verifyComplete();

        StepVerifier.create(reactiveRepository.findById(homer.getId())).expectNextCount(0).verifyComplete();
        StepVerifier.create(reactiveRepository.findById(marge.getId())).expectNext(marge).verifyComplete();


        // cleanup
        StepVerifier.create(reactiveRepository.save(homer))
            .expectNextCount(1)
            .verifyComplete();
    }

    @Test
    public void deleteByIdPublisher_ShouldSkipNonexistent() {
        StepVerifier.create(reactiveRepository.deleteById(Flux.just("non-existent-id")))
            .verifyComplete();
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void deleteByIdPublisher_ShouldRejectNullObject() {
        //noinspection unchecked,rawtypes
        assertThatThrownBy(() -> reactiveRepository.deleteById((Publisher) null).block())
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void delete_ShouldDeleteExistent() {
        StepVerifier.create(reactiveRepository.delete(marge)).verifyComplete();

        StepVerifier.create(reactiveRepository.findById(marge.getId())).expectNextCount(0).verifyComplete();

        // cleanup
        StepVerifier.create(reactiveRepository.save(marge))
            .expectNextCount(1)
            .verifyComplete();
    }

    @Test
    public void delete_ShouldSkipNonexistent() {
        Customer nonExistentCustomer = Customer.builder().id(nextId()).firstName("Bart").lastName("Simpson").age(15)
            .build();

        StepVerifier.create(reactiveRepository.delete(nonExistentCustomer))
            .verifyComplete();
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void delete_ShouldRejectNullObject() {
        assertThatThrownBy(() -> reactiveRepository.delete(null).block())
            .isInstanceOf(IllegalArgumentException.class);

    }

    @Test
    public void deleteAllIterable_ShouldDeleteExistent() {
        reactiveRepository.deleteAll(asList(homer, marge)).block();

        StepVerifier.create(reactiveRepository.findById(homer.getId())).expectNextCount(0).verifyComplete();
        StepVerifier.create(reactiveRepository.findById(marge.getId())).expectNextCount(0).verifyComplete();

        // cleanup
        StepVerifier.create(reactiveRepository.saveAll(Flux.just(marge, homer)))
            .expectNextCount(2)
            .verifyComplete();
    }

    @Test
    public void deleteAllIterable_ShouldSkipNonexistentAndThrowException() {
        Customer nonExistentCustomer = Customer.builder().id(nextId()).firstName("Bart").lastName("Simpson").age(15)
            .build();

        assertThatThrownBy(() -> reactiveRepository.deleteAll(asList(homer, nonExistentCustomer, marge)).block())
            .isInstanceOf(AerospikeException.BatchRecordArray.class);
        StepVerifier.create(reactiveRepository.findById(homer.getId())).expectNextCount(0).verifyComplete();
        StepVerifier.create(reactiveRepository.findById(marge.getId())).expectNextCount(0).verifyComplete();

        // cleanup
        StepVerifier.create(reactiveRepository.saveAll(Flux.just(marge, homer)))
            .expectNextCount(2)
            .verifyComplete();
    }

    @Test
    public void deleteAllIterable_ShouldRejectNullObject() {
        List<Customer> entities = asList(homer, null, marge);

        assertThatThrownBy(() -> reactiveRepository.deleteAll(entities).block())
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void deleteAllPublisher_ShouldDeleteExistent() {
        reactiveRepository.deleteAll(Flux.just(homer, marge)).block();

        StepVerifier.create(reactiveRepository.findById(homer.getId())).expectNextCount(0).verifyComplete();
        StepVerifier.create(reactiveRepository.findById(marge.getId())).expectNextCount(0).verifyComplete();

        // cleanup
        StepVerifier.create(reactiveRepository.saveAll(Flux.just(marge, homer)))
            .expectNextCount(2)
            .verifyComplete();
    }

    @Test
    public void deleteAllPublisher_ShouldNotSkipNonexistent() {
        Customer nonExistentCustomer = Customer.builder().id(nextId()).firstName("Bart").lastName("Simpson").age(15)
            .build();

        reactiveRepository.deleteAll(Flux.just(homer, nonExistentCustomer, marge))
            .block();

        StepVerifier.create(reactiveRepository.findById(homer.getId())).expectNextCount(0).verifyComplete();
        StepVerifier.create(reactiveRepository.findById(marge.getId())).expectNextCount(0).verifyComplete();

        // cleanup
        StepVerifier.create(reactiveRepository.saveAll(Flux.just(marge, homer)))
            .expectNextCount(2)
            .verifyComplete();
    }

    @Test
    public void deleteAllById_ShouldDelete() {
        reactiveRepository.deleteAllById(asList(homer.getId(), marge.getId()))
            .block();

        StepVerifier.create(reactiveRepository.findById(homer.getId())).expectNextCount(0).verifyComplete();
        StepVerifier.create(reactiveRepository.findById(marge.getId())).expectNextCount(0).verifyComplete();

        // cleanup
        StepVerifier.create(reactiveRepository.saveAll(Flux.just(marge, homer)))
            .expectNextCount(2)
            .verifyComplete();
    }

    @Test
    public void deleteAll_ShouldDelete() {
        reactiveRepository.deleteAll().block();

        StepVerifier.create(reactiveRepository.findById(homer.getId())).expectNextCount(0).verifyComplete();
        StepVerifier.create(reactiveRepository.findById(marge.getId())).expectNextCount(0).verifyComplete();

        // cleanup
        reactiveBlockingAerospikeTestOperations.saveAll(reactiveRepository, allCustomers);
    }

    @Test
    public void deleteById_AND_SimpleProperty() {
        StepVerifier.create(reactiveRepository.findAllById(List.of(bart.getId())).collectList())
            .expectNextMatches(list -> list.size() == 1 && list.contains(bart))
            .verifyComplete();

        QueryParam id = QueryParam.of(bart.getId());
        QueryParam firstName = QueryParam.of("FirstName");
        // no records satisfying the condition
        StepVerifier.create(reactiveRepository.deleteByIdAndFirstName(id, firstName))
            .expectComplete()
            .verify();

        // no records get deleted
        StepVerifier.create(reactiveRepository.findAllById(List.of(bart.getId())).collectList())
            .expectNextMatches(list -> list.size() == 1 && list.contains(bart))
            .verifyComplete();

        // 1 record satisfying the condition
        firstName = QueryParam.of(bart.getFirstName());
        StepVerifier.create(reactiveRepository.deleteByIdAndFirstName(id, firstName))
            .expectComplete()
            .verify();

        // 1 record gets deleted
        StepVerifier.create(reactiveRepository.findAllById(List.of(bart.getId())).collectList())
            .expectNextMatches(List::isEmpty)
            .verifyComplete();

        // cleanup
        StepVerifier.create(reactiveRepository.save(bart))
            .expectNextCount(1)
            .verifyComplete();
    }
}
