package org.springframework.data.aerospike.repository.reactive;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.ReactiveCustomerRepository;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.List;

public class ReactiveAerospikeRepositoryCountRelatedTests extends BaseReactiveIntegrationTests {

    @Autowired
    ReactiveCustomerRepository customerRepo;

    private Customer customer1, customer2;

    @BeforeEach
    public void setUp() {
        customer1 = Customer.builder().id(nextId()).firstName("Philip J.").lastName("Fry").age(1029).build();
        customer2 = Customer.builder().id(nextId()).firstName("Leela").lastName("Turanga").age(29).build();
        StepVerifier.create(customerRepo.saveAll(Flux.just(customer1, customer2))).expectNextCount(2).verifyComplete();
    }

    @Test
    public void countByAgeBetween() {
        StepVerifier.create(customerRepo.countByAgeBetween(20, 1100))
            .expectNext(2L)
            .verifyComplete();
    }

    @Test
    public void countByIdAndFirstNameIn() {
        QueryParam ids = QueryParam.of(List.of(customer1.getId(), customer2.getId()));
        QueryParam firstNames = QueryParam.of(List.of(customer1.getFirstName(), customer2.getFirstName(), "FirstName"));
        StepVerifier.create(customerRepo.countByIdAndFirstNameIn(ids, firstNames))
            .expectNext(2L)
            .verifyComplete();

        firstNames = QueryParam.of(List.of("FirstName"));
        StepVerifier.create(customerRepo.countByIdAndFirstNameIn(ids, firstNames))
            .expectNext(0L)
            .verifyComplete();
    }
}
