package org.springframework.data.aerospike.repository.reactive;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.ReactiveCustomerRepository;
import org.springframework.test.context.TestPropertySource;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.springframework.data.aerospike.query.cache.IndexRefresher.INDEX_CACHE_REFRESH_SECONDS;

/**
 * @author Igor Ermolenko
 */
@TestPropertySource(properties = {INDEX_CACHE_REFRESH_SECONDS + " = 0", "createIndexesOnStartup = false"})
// this test class does not require secondary indexes created on startup
public class ReactiveAerospikeRepositoryExistRelatedTests extends BaseReactiveIntegrationTests {

    @Autowired
    ReactiveCustomerRepository customerRepo;

    private Customer customer1, customer2;

    @BeforeEach
    public void setUp() {
        customer1 = Customer.builder().id(nextId()).firstName("Homer").lastName("Simpson").age(42).build();
        customer2 = Customer.builder().id(nextId()).firstName("Marge").lastName("Simpson").age(39).build();
        StepVerifier.create(customerRepo.saveAll(Flux.just(customer1, customer2))).expectNextCount(2).verifyComplete();
    }

    @Test
    public void existsById_ShouldReturnTrueWhenExists() {
        StepVerifier.create(customerRepo.existsById(customer2.getId()).subscribeOn(Schedulers.parallel()))
            .expectNext(true).verifyComplete();
    }

    @Test
    public void existsById_ShouldReturnFalseWhenNotExists() {
        StepVerifier.create(customerRepo.existsById("non-existent-id").subscribeOn(Schedulers.parallel()))
            .expectNext(false).verifyComplete();
    }

    @Test
    public void existsByIdPublisher_ShouldReturnTrueWhenExists() {
        StepVerifier.create(customerRepo.existsById(Flux.just(customer1.getId())).subscribeOn(Schedulers.parallel()))
            .expectNext(true).verifyComplete();
    }

    @Test
    public void existsByIdPublisher_ShouldReturnFalseWhenNotExists() {
        StepVerifier.create(customerRepo.existsById(Flux.just("non-existent-id")).subscribeOn(Schedulers.parallel()))
            .expectNext(false).verifyComplete();
    }

    @Test
    public void existsByIdPublisher_ShouldCheckOnlyFirstElement() {
        StepVerifier.create(customerRepo.existsById(Flux.just(customer1.getId(), "non-existent-id"))
                .subscribeOn(Schedulers.parallel()))
            .expectNext(true).verifyComplete();
    }
}
