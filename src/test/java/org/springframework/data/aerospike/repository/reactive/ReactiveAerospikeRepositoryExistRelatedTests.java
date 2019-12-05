package org.springframework.data.aerospike.repository.reactive;


import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.ReactiveCustomerRepository;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;


/**
 * @author Igor Ermolenko
 */
public class ReactiveAerospikeRepositoryExistRelatedTests extends BaseReactiveIntegrationTests {

    @Autowired
    ReactiveCustomerRepository customerRepo;

    private Customer customer1, customer2;

    @Before
    public void setUp() {
        customer1 = Customer.builder().id(nextId()).firstname("Homer").lastname("Simpson").age(42).build();
        customer2 = Customer.builder().id(nextId()).firstname("Marge").lastname("Simpson").age(39).build();
        StepVerifier.create(customerRepo.saveAll(Flux.just(customer1, customer2))).expectNextCount(2).verifyComplete();
    }


    @Test
    public void existsById_ShouldReturnTrueWhenExists() {
        StepVerifier.create(customerRepo.existsById(customer2.getId())).expectNext(true).verifyComplete();
    }

    @Test
    public void existsById_ShouldReturnFalseWhenNotExists() {
        StepVerifier.create(customerRepo.existsById("non-existent-id")).expectNext(false).verifyComplete();
    }

    @Test
    public void existsByIdPublisher_ShouldReturnTrueWhenExists() {
        StepVerifier.create(customerRepo.existsById(Flux.just(customer1.getId()))).expectNext(true).verifyComplete();
    }

    @Test
    public void existsByIdPublisher_ShouldReturnFalseWhenNotExists() {
        StepVerifier.create(customerRepo.existsById(Flux.just("non-existent-id"))).expectNext(false).verifyComplete();
    }

    @Test
    public void existsByIdPublisher_ShouldCheckOnlyFirstElement() {
        StepVerifier.create(customerRepo.existsById(Flux.just(customer1.getId(), "non-existent-id"))).expectNext(true).verifyComplete();
    }

}
