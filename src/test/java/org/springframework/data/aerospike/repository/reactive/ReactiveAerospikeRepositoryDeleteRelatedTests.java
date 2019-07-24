package org.springframework.data.aerospike.repository.reactive;

import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.ReactiveCustomerRepository;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.List;

import static java.util.Arrays.asList;

/**
 * @author Igor Ermolenko
 */
public class ReactiveAerospikeRepositoryDeleteRelatedTests extends BaseIntegrationTests {

    @Autowired
    ReactiveCustomerRepository customerRepo;

    private Customer customer1, customer2;

    @Before
    public void setUp() {
        customer1 = Customer.builder().id(nextId()).firstname("Homer").lastname("Simpson").age(42).build();
        customer2 = Customer.builder().id(nextId()).firstname("Marge").lastname("Simpson").age(39).build();
        StepVerifier.create(customerRepo.saveAll(Flux.just(customer1, customer2))).expectNextCount(2).verifyComplete();

        StepVerifier.create(customerRepo.findById(customer1.getId())).expectNext(customer1).verifyComplete();
        StepVerifier.create(customerRepo.findById(customer2.getId())).expectNext(customer2).verifyComplete();
    }


    @Test
    public void deleteById_ShouldDeleteExistent() {
        StepVerifier.create(customerRepo.deleteById(customer2.getId())).verifyComplete();

        StepVerifier.create(customerRepo.findById(customer2.getId())).expectNextCount(0).verifyComplete();
    }

    @Test
    public void deleteById_ShouldSkipNonexistent() {
        StepVerifier.create(customerRepo.deleteById("non-existent-id")).verifyComplete();
    }

    @Test(expected = IllegalArgumentException.class)
    public void deleteById_ShouldRejectsNullObject() {
        customerRepo.deleteById((String) null).block();
    }

    @Test
    public void deleteByIdPublisher_ShouldDeleteOnlyFirstElement() {
        StepVerifier.create(customerRepo.deleteById(Flux.just(customer1.getId(), customer2.getId()))).verifyComplete();

        StepVerifier.create(customerRepo.findById(customer1.getId())).expectNextCount(0).verifyComplete();
        StepVerifier.create(customerRepo.findById(customer2.getId())).expectNext(customer2).verifyComplete();
    }

    @Test
    public void deleteByIdPublisher_ShouldSkipNonexistent() {
        StepVerifier.create(customerRepo.deleteById(Flux.just("non-existent-id"))).verifyComplete();
    }

    @Test(expected = IllegalArgumentException.class)
    public void deleteByIdPublisher_ShouldRejectsNullObject() {
        customerRepo.deleteById((Publisher) null).block();
    }

    @Test
    public void delete_ShouldDeleteExistent() {
        StepVerifier.create(customerRepo.delete(customer2)).verifyComplete();

        StepVerifier.create(customerRepo.findById(customer2.getId())).expectNextCount(0).verifyComplete();
    }

    @Test
    public void delete_ShouldSkipNonexistent() {
        Customer nonExistentCustomer = Customer.builder().id(nextId()).firstname("Bart").lastname("Simpson").age(15).build();

        StepVerifier.create(customerRepo.delete(nonExistentCustomer)).verifyComplete();
    }

    @Test(expected = IllegalArgumentException.class)
    public void delete_ShouldRejectsNullObject() {
        customerRepo.delete(null).block();
    }

    @Test
    public void deleteAllIterable_ShouldDeleteExistent() {
        customerRepo.deleteAll(asList(customer1, customer2)).block();

        StepVerifier.create(customerRepo.findById(customer1.getId())).expectNextCount(0).verifyComplete();
        StepVerifier.create(customerRepo.findById(customer2.getId())).expectNextCount(0).verifyComplete();
    }

    @Test
    public void deleteAllIterable_ShouldSkipNonexistent() {
        Customer nonExistentCustomer = Customer.builder().id(nextId()).firstname("Bart").lastname("Simpson").age(15).build();

        customerRepo.deleteAll(asList(customer1, nonExistentCustomer, customer2)).block();

        StepVerifier.create(customerRepo.findById(customer1.getId())).expectNextCount(0).verifyComplete();
        StepVerifier.create(customerRepo.findById(customer2.getId())).expectNextCount(0).verifyComplete();
    }

    @Test(expected = IllegalArgumentException.class)
    public void deleteAllIterable_ShouldRejectsNullObject() {
        List<Customer> entities = asList(customer1, null, customer2);
        customerRepo.deleteAll(entities).block();
    }


    @Test
    public void deleteAllPublisher_ShouldDeleteExistent() {
        customerRepo.deleteAll(Flux.just(customer1, customer2)).block();

        StepVerifier.create(customerRepo.findById(customer1.getId())).expectNextCount(0).verifyComplete();
        StepVerifier.create(customerRepo.findById(customer2.getId())).expectNextCount(0).verifyComplete();
    }

    @Test
    public void deleteAllPublisher_ShouldSkipNonexistent() {
        Customer nonExistentCustomer = Customer.builder().id(nextId()).firstname("Bart").lastname("Simpson").age(15).build();

        customerRepo.deleteAll(Flux.just(customer1, nonExistentCustomer, customer2)).block();

        StepVerifier.create(customerRepo.findById(customer1.getId())).expectNextCount(0).verifyComplete();
        StepVerifier.create(customerRepo.findById(customer2.getId())).expectNextCount(0).verifyComplete();
    }

}