package org.springframework.data.aerospike.repository.reactive;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.ReactiveCustomerRepository;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Igor Ermolenko
 */
public class ReactiveAerospikeRepositoryDeleteRelatedTests extends BaseReactiveIntegrationTests {

    @Autowired
    ReactiveCustomerRepository customerRepo;

    private Customer customer1, customer2;

    @BeforeEach
    public void setUp() {
        customer1 = Customer.builder().id(nextId()).firstname("Homer").lastname("Simpson").age(42).build();
        customer2 = Customer.builder().id(nextId()).firstname("Marge").lastname("Simpson").age(39).build();
        StepVerifier.create(customerRepo.saveAll(Flux.just(customer1, customer2))).expectNextCount(2).verifyComplete();

        StepVerifier.create(customerRepo.findById(customer1.getId())).expectNext(customer1).verifyComplete();
        StepVerifier.create(customerRepo.findById(customer2.getId())).expectNext(customer2).verifyComplete();
    }

    @Test
    public void deleteById_ShouldDeleteExistent() {
        StepVerifier.create(customerRepo.deleteById(customer2.getId()).subscribeOn(Schedulers.parallel())).verifyComplete();

        StepVerifier.create(customerRepo.findById(customer2.getId())).expectNextCount(0).verifyComplete();
    }

    @Test
    public void deleteById_ShouldSkipNonexistent() {
        StepVerifier.create(customerRepo.deleteById("non-existent-id").subscribeOn(Schedulers.parallel())).verifyComplete();
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void deleteById_ShouldRejectsNullObject() {
        assertThatThrownBy(() -> customerRepo.deleteById((String) null).block())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void deleteByIdPublisher_ShouldDeleteOnlyFirstElement() {
        StepVerifier.create(customerRepo.deleteById(Flux.just(customer1.getId(), customer2.getId()))
                .subscribeOn(Schedulers.parallel()))
                .verifyComplete();

        StepVerifier.create(customerRepo.findById(customer1.getId())).expectNextCount(0).verifyComplete();
        StepVerifier.create(customerRepo.findById(customer2.getId())).expectNext(customer2).verifyComplete();
    }

    @Test
    public void deleteByIdPublisher_ShouldSkipNonexistent() {
        StepVerifier.create(customerRepo.deleteById(Flux.just("non-existent-id"))
                .subscribeOn(Schedulers.parallel()))
                .verifyComplete();
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void deleteByIdPublisher_ShouldRejectsNullObject() {
        assertThatThrownBy(() -> customerRepo.deleteById((Publisher) null).block())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void delete_ShouldDeleteExistent() {
        StepVerifier.create(customerRepo.delete(customer2).subscribeOn(Schedulers.parallel())).verifyComplete();

        StepVerifier.create(customerRepo.findById(customer2.getId())).expectNextCount(0).verifyComplete();
    }

    @Test
    public void delete_ShouldSkipNonexistent() {
        Customer nonExistentCustomer = Customer.builder().id(nextId()).firstname("Bart").lastname("Simpson").age(15).build();

        StepVerifier.create(customerRepo.delete(nonExistentCustomer).subscribeOn(Schedulers.parallel()))
                .verifyComplete();
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void delete_ShouldRejectsNullObject() {
        assertThatThrownBy(() -> customerRepo.delete(null).block())
                .isInstanceOf(IllegalArgumentException.class);

    }

    @Test
    public void deleteAllIterable_ShouldDeleteExistent() {
        customerRepo.deleteAll(asList(customer1, customer2)).subscribeOn(Schedulers.parallel()).block();

        StepVerifier.create(customerRepo.findById(customer1.getId())).expectNextCount(0).verifyComplete();
        StepVerifier.create(customerRepo.findById(customer2.getId())).expectNextCount(0).verifyComplete();
    }

    @Test
    public void deleteAllIterable_ShouldSkipNonexistent() {
        Customer nonExistentCustomer = Customer.builder().id(nextId()).firstname("Bart").lastname("Simpson").age(15).build();

        customerRepo.deleteAll(asList(customer1, nonExistentCustomer, customer2)).subscribeOn(Schedulers.parallel()).block();

        StepVerifier.create(customerRepo.findById(customer1.getId())).expectNextCount(0).verifyComplete();
        StepVerifier.create(customerRepo.findById(customer2.getId())).expectNextCount(0).verifyComplete();
    }

    @Test
    public void deleteAllIterable_ShouldRejectsNullObject() {
        List<Customer> entities = asList(customer1, null, customer2);

        assertThatThrownBy(() -> customerRepo.deleteAll(entities).subscribeOn(Schedulers.parallel()).block())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void deleteAllPublisher_ShouldDeleteExistent() {
        customerRepo.deleteAll(Flux.just(customer1, customer2)).subscribeOn(Schedulers.parallel()).block();

        StepVerifier.create(customerRepo.findById(customer1.getId())).expectNextCount(0).verifyComplete();
        StepVerifier.create(customerRepo.findById(customer2.getId())).expectNextCount(0).verifyComplete();
    }

    @Test
    public void deleteAllPublisher_ShouldSkipNonexistent() {
        Customer nonExistentCustomer = Customer.builder().id(nextId()).firstname("Bart").lastname("Simpson").age(15).build();

        customerRepo.deleteAll(Flux.just(customer1, nonExistentCustomer, customer2)).subscribeOn(Schedulers.parallel()).block();

        StepVerifier.create(customerRepo.findById(customer1.getId())).expectNextCount(0).verifyComplete();
        StepVerifier.create(customerRepo.findById(customer2.getId())).expectNextCount(0).verifyComplete();
    }

    @Test
    public void deleteAllById_ShouldDelete() {
        customerRepo.deleteAllById(asList(customer1.getId(), customer2.getId())).subscribeOn(Schedulers.parallel()).block();

        StepVerifier.create(customerRepo.findById(customer1.getId())).expectNextCount(0).verifyComplete();
        StepVerifier.create(customerRepo.findById(customer2.getId())).expectNextCount(0).verifyComplete();
    }

    @Test
    public void deleteAll_ShouldDelete() {
        customerRepo.deleteAll().block();

        StepVerifier.create(customerRepo.findById(customer1.getId())).expectNextCount(0).verifyComplete();
        StepVerifier.create(customerRepo.findById(customer2.getId())).expectNextCount(0).verifyComplete();
    }
}