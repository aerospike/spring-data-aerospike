package org.springframework.data.aerospike.repository.reactive;


import com.aerospike.client.query.IndexType;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.ReactiveCompositeObjectRepository;
import org.springframework.data.aerospike.sample.ReactiveCustomerRepository;
import org.springframework.data.domain.Sort;
import reactor.core.publisher.Flux;
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
public class ReactiveAerospikeRepositoryFindRelatedTests extends BaseIntegrationTests {
    @Autowired
    ReactiveCustomerRepository customerRepo;
    @Autowired
    ReactiveCompositeObjectRepository compositeRepo;

    private Customer customer1, customer2, customer3, customer4;


    @Before
    public void setUp() {
        cleanDb();

        customer1 = Customer.builder().id(nextId()).firstname("Homer").lastname("Simpson").age(42).build();
        customer2 = Customer.builder().id(nextId()).firstname("Marge").lastname("Simpson").age(39).build();
        customer3 = Customer.builder().id(nextId()).firstname("Bart").lastname("Simpson").age(15).build();
        customer4 = Customer.builder().id(nextId()).firstname("Matt").lastname("Groening").age(65).build();

        createIndexIfNotExists(Customer.class, "customer_first_name_index", "firstname", IndexType.STRING);
        createIndexIfNotExists(Customer.class, "customer_last_name_index", "lastname", IndexType.STRING);
        createIndexIfNotExists(Customer.class, "customer_age_index", "age", IndexType.NUMERIC);

        StepVerifier.create(customerRepo.save(customer1)).expectNext(customer1).verifyComplete();
        StepVerifier.create(customerRepo.save(customer2)).expectNext(customer2).verifyComplete();
        StepVerifier.create(customerRepo.save(customer3)).expectNext(customer3).verifyComplete();
        StepVerifier.create(customerRepo.save(customer4)).expectNext(customer4).verifyComplete();
    }

    @Test
    public void findById_ShouldReturnExistent() {
        StepVerifier.create(customerRepo.findById(customer2.getId())).consumeNextWith(actual ->
                assertThat(actual).isEqualTo(customer2)
        ).verifyComplete();
    }

    @Test
    public void findById_ShouldNotReturnNotExistent() {
        StepVerifier.create(customerRepo.findById("non-existent-id")).expectNextCount(0).verifyComplete();
    }

    @Test
    public void findByIdPublisher_ShouldReturnFirst() {
        Publisher<String> ids = Flux.just(customer2.getId(), customer4.getId());

        StepVerifier.create(customerRepo.findById(ids)).consumeNextWith(actual ->
                assertThat(actual).isEqualTo(customer2)
        ).verifyComplete();
    }

    @Test
    public void findByIdPublisher_NotReturnFirstNotExistent() {
        Publisher<String> ids = Flux.just("non-existent-id", customer2.getId(), customer4.getId());

        StepVerifier.create(customerRepo.findById(ids)).expectNextCount(0).verifyComplete();
    }

    @Test
    public void findAll_ShouldReturnAll() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findAll()),
                customers -> assertThat(customers).containsOnly(customer1, customer2, customer3, customer4));
    }

    @Test
    public void findAllByIDsIterable_ShouldReturnAllExistent() {
        Iterable<String> ids = asList(customer2.getId(), "non-existent-id", customer4.getId());
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findAllById(ids)),
                customers -> assertThat(customers).containsOnly(customer2, customer4));

    }

    @Test
    public void findAllByIDsPublisher_ShouldReturnAllExistent() {
        Publisher<String> ids = Flux.just(customer1.getId(), customer2.getId(), customer4.getId(), "non-existent-id");
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findAllById(ids)),
                customers -> assertThat(customers).containsOnly(customer1, customer2, customer4));
    }

    @Test
    public void findByLastname_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByLastname("Simpson")),
                customers -> assertThat(customers).containsOnly(customer1, customer2, customer3));
    }

    @Test
    public void findByLastnameName_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByLastnameNot("Simpson")),
                customers -> assertThat(customers).containsOnly(customer4));
    }

    @Test
    public void findOneByLastname_ShouldWorkProperly() {
        StepVerifier.create(customerRepo.findOneByLastname("Groening")).consumeNextWith(actual ->
                assertThat(actual).isEqualTo(customer4)
        ).verifyComplete();
    }

    @Test
    public void findByLastnameOrderByFirstnameAsc_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByLastnameOrderByFirstnameAsc("Simpson")),
                customers -> assertThat(customers).containsExactly(customer3, customer1, customer2));
    }

    @Test
    public void findByLastnameOrderByFirstnameDesc_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByLastnameOrderByFirstnameDesc("Simpson")),
                customers -> assertThat(customers).containsExactly(customer2, customer1, customer3));
    }

    @Test
    public void findByFirstnameEndsWith_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByFirstnameEndsWith("t")),
                customers -> assertThat(customers).containsOnly(customer3, customer4));
    }

    @Test
    public void findByFirstnameStartsWithOrderByAgeAsc_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByFirstnameStartsWithOrderByAgeAsc("Ma")),
                customers -> assertThat(customers).containsExactly(customer2, customer4));
    }

    @Test
    public void findByAgeLessThan_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByAgeLessThan(40, Sort.by(asc("firstname")))),
                customers -> assertThat(customers).containsExactly(customer3, customer2)
        );
    }

    @Test
    public void findByFirstnameIn_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByFirstnameIn(asList("Matt", "Homer"))),
                customers -> assertThat(customers).containsOnly(customer1, customer4));
    }

    @Test
    public void findByFirstnameAndLastname_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByFirstnameAndLastname("Bart", "Simpson")),
                customers -> assertThat(customers).containsOnly(customer3));
    }

    @Test
    public void findOneByFirstnameAndLastname_ShouldWorkProperly() {
        StepVerifier.create(customerRepo.findOneByFirstnameAndLastname("Bart", "Simpson")).consumeNextWith(actual ->
                assertThat(actual).isEqualTo(customer3)
        ).verifyComplete();
    }

    @Test
    public void findByLastnameAndAge_ShouldWorkProperly() {
        StepVerifier.create(customerRepo.findByLastnameAndAge("Simpson", 15)).consumeNextWith(actual ->
                assertThat(actual).isEqualTo(customer3)
        ).verifyComplete();
    }

    @Test
    public void findByAgeBetween_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByAgeBetween(10, 40)),
                customers -> assertThat(customers).containsOnly(customer2, customer3));
    }

    @Test
    public void findByFirstnameContains_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByFirstnameContains("ar")),
                customers -> assertThat(customers).containsOnly(customer2, customer3));
    }

    @Test
    public void findByFirstnameContainingIgnoreCase_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByFirstnameContainingIgnoreCase("m")),
                customers -> assertThat(customers).containsOnly(customer1, customer2, customer4));
    }

    @Test
    public void findByAgeBetweenAndLastname_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByAgeBetweenAndLastname(30, 70,"Simpson")),
                customers -> assertThat(customers).containsOnly(customer1, customer2));
    }

    @Test
    public void findByAgeBetweenOrderByFirstnameDesc_ShouldWorkProperly() {
        assertConsumedCustomers(
                StepVerifier.create(customerRepo.findByAgeBetweenOrderByFirstnameDesc(30, 70)),
                customers -> assertThat(customers).containsExactly(customer4, customer2, customer1));
    }



    private void assertConsumedCustomers(StepVerifier.FirstStep<Customer> step, Consumer<Collection<Customer>> assertion) {
        step.recordWith(ArrayList::new)
                .thenConsumeWhile(customer -> true)
                .consumeRecordedWith(assertion)
                .verifyComplete();
    }

}

      