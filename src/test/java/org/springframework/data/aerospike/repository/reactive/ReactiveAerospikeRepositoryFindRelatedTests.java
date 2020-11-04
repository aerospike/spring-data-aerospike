package org.springframework.data.aerospike.repository.reactive;


import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.ReactiveCustomerRepository;
import org.springframework.data.domain.Sort;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.domain.Sort.Order.asc;


/**
 * @author Igor Ermolenko
 */
public class ReactiveAerospikeRepositoryFindRelatedTests extends BaseReactiveIntegrationTests {
    @Autowired
    ReactiveCustomerRepository customerRepo;

    private Customer customer1, customer2, customer3, customer4;


    @BeforeEach
    public void setUp() {
        StepVerifier.create(deleteAll()).verifyComplete();

        customer1 = Customer.builder().id(nextId()).firstname("Homer").lastname("Simpson").age(42).build();
        customer2 = Customer.builder().id(nextId()).firstname("Marge").lastname("Simpson").age(39).build();
        customer3 = Customer.builder().id(nextId()).firstname("Bart").lastname("Simpson").age(15).build();
        customer4 = Customer.builder().id(nextId()).firstname("Matt").lastname("Groening").age(65).build();

        additionalAerospikeTestOperations.createIndexIfNotExists(Customer.class, "customer_first_name_index", "firstname", IndexType.STRING);
        additionalAerospikeTestOperations.createIndexIfNotExists(Customer.class, "customer_last_name_index", "lastname", IndexType.STRING);
        additionalAerospikeTestOperations.createIndexIfNotExists(Customer.class, "customer_age_index", "age", IndexType.NUMERIC);

        StepVerifier.create(customerRepo.saveAll(Flux.just(customer1, customer2, customer3, customer4))).expectNextCount(4).verifyComplete();
    }

    @Test
    public void findById_ShouldReturnExistent() {
        StepVerifier.create(customerRepo.findById(customer2.getId())
                .subscribeOn(Schedulers.parallel())).consumeNextWith(actual ->
                assertThat(actual).isEqualTo(customer2)
        ).verifyComplete();
    }

    @Test
    public void findById_ShouldNotReturnNotExistent() {
        StepVerifier.create(customerRepo.findById("non-existent-id")
                .subscribeOn(Schedulers.parallel()))
                .expectNextCount(0).verifyComplete();
    }

    @Test
    public void findByIdPublisher_ShouldReturnFirst() {
        Publisher<String> ids = Flux.just(customer2.getId(), customer4.getId());

        StepVerifier.create(customerRepo.findById(ids)
                .subscribeOn(Schedulers.parallel()))
                .consumeNextWith(actual ->
                        assertThat(actual).isEqualTo(customer2)
                ).verifyComplete();
    }

    @Test
    public void findByIdPublisher_NotReturnFirstNotExistent() {
        Publisher<String> ids = Flux.just("non-existent-id", customer2.getId(), customer4.getId());

        StepVerifier.create(customerRepo.findById(ids)
                .subscribeOn(Schedulers.parallel()))
                .expectNextCount(0).verifyComplete();
    }

    @Test
    public void findAll_ShouldReturnAll() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findAll()
                        .subscribeOn(Schedulers.parallel())),
                customers -> assertThat(customers).containsOnly(customer1, customer2, customer3, customer4));
    }

    @Test
    public void findAllByIDsIterable_ShouldReturnAllExistent() {
        Iterable<String> ids = asList(customer2.getId(), "non-existent-id", customer4.getId());
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findAllById(ids)
                        .subscribeOn(Schedulers.parallel())),
                customers -> assertThat(customers).containsOnly(customer2, customer4));

    }

    @Test
    public void findAllByIDsPublisher_ShouldReturnAllExistent() {
        Publisher<String> ids = Flux.just(customer1.getId(), customer2.getId(), customer4.getId(), "non-existent-id");
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findAllById(ids)
                        .subscribeOn(Schedulers.parallel())),
                customers -> assertThat(customers).containsOnly(customer1, customer2, customer4));
    }

    @Test
    public void findByLastname_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByLastname("Simpson")
                        .subscribeOn(Schedulers.parallel())),
                customers -> assertThat(customers).containsOnly(customer1, customer2, customer3));
    }

    @Test
    public void findByLastnameName_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByLastnameNot("Simpson")
                        .subscribeOn(Schedulers.parallel())),
                customers -> assertThat(customers).containsOnly(customer4));
    }

    @Test
    public void findOneByLastname_ShouldWorkProperly() {
        StepVerifier.create(customerRepo.findOneByLastname("Groening")
                .subscribeOn(Schedulers.parallel()))
                .consumeNextWith(actual -> assertThat(actual).isEqualTo(customer4)
                ).verifyComplete();
    }

    @Test
    public void findByLastnameOrderByFirstnameAsc_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByLastnameOrderByFirstnameAsc("Simpson")
                        .subscribeOn(Schedulers.parallel())),
                customers -> assertThat(customers).containsExactly(customer3, customer1, customer2));
    }

    @Test
    public void findByLastnameOrderByFirstnameDesc_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByLastnameOrderByFirstnameDesc("Simpson")
                        .subscribeOn(Schedulers.parallel())),
                customers -> assertThat(customers).containsExactly(customer2, customer1, customer3));
    }

    @Test
    public void findByFirstnameEndsWith_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByFirstnameEndsWith("t")
                        .subscribeOn(Schedulers.parallel())),
                customers -> assertThat(customers).containsOnly(customer3, customer4));
    }

    @Test
    public void findByFirstnameStartsWithOrderByAgeAsc_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByFirstnameStartsWithOrderByAgeAsc("Ma")
                        .subscribeOn(Schedulers.parallel())),
                customers -> assertThat(customers).containsExactly(customer2, customer4));
    }

    @Test
    public void findByAgeLessThan_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByAgeLessThan(40, Sort.by(asc("firstname")))
                        .subscribeOn(Schedulers.parallel())),
                customers -> assertThat(customers).containsExactly(customer3, customer2)
        );
    }

    @Test
    public void findByFirstnameIn_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByFirstnameIn(asList("Matt", "Homer"))
                        .subscribeOn(Schedulers.parallel())),
                customers -> assertThat(customers).containsOnly(customer1, customer4));
    }

    @Test
    public void findByFirstnameAndLastname_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByFirstnameAndLastname("Bart", "Simpson")
                        .subscribeOn(Schedulers.parallel())),
                customers -> assertThat(customers).containsOnly(customer3));
    }

    @Test
    public void findOneByFirstnameAndLastname_ShouldWorkProperly() {
        StepVerifier.create(customerRepo.findOneByFirstnameAndLastname("Bart", "Simpson")
                .subscribeOn(Schedulers.parallel()))
                .consumeNextWith(actual ->assertThat(actual).isEqualTo(customer3)
                ).verifyComplete();
    }

    @Test
    public void findByLastnameAndAge_ShouldWorkProperly() {
        StepVerifier.create(customerRepo.findByLastnameAndAge("Simpson", 15)
                .subscribeOn(Schedulers.parallel()))
                .consumeNextWith(actual -> assertThat(actual).isEqualTo(customer3)
                ).verifyComplete();
    }

    @Test
    public void findByAgeBetween_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByAgeBetween(10, 40)
                        .subscribeOn(Schedulers.parallel())),
                customers -> assertThat(customers).containsOnly(customer2, customer3));
    }

    @Test
    public void findByFirstnameContains_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByFirstnameContains("ar")
                        .subscribeOn(Schedulers.parallel())),
                customers -> assertThat(customers).containsOnly(customer2, customer3));
    }

    @Test
    public void findByFirstnameContainingIgnoreCase_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByFirstnameContainingIgnoreCase("m")
                        .subscribeOn(Schedulers.parallel())),
                customers -> assertThat(customers).containsOnly(customer1, customer2, customer4));
    }

    @Test
    public void findByAgeBetweenAndLastname_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByAgeBetweenAndLastname(30, 70,"Simpson")
                        .subscribeOn(Schedulers.parallel())),
                customers -> assertThat(customers).containsOnly(customer1, customer2));
    }

    @Test
    public void findByAgeBetweenOrderByFirstnameDesc_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByAgeBetweenOrderByFirstnameDesc(30, 70)
                        .subscribeOn(Schedulers.parallel())),
                customers -> assertThat(customers).containsExactly(customer4, customer2, customer1));
    }

    private void assertConsumedCustomers(StepVerifier.FirstStep<Customer> step, Consumer<Collection<Customer>> assertion) {
        step.recordWith(ArrayList::new)
                .thenConsumeWhile(customer -> true)
                .consumeRecordedWith(assertion)
                .verifyComplete();
    }

    private Mono<Void> deleteAll() {
        return customerRepo.findAll().flatMap(a -> customerRepo.delete(a)).then();
    }
}

      