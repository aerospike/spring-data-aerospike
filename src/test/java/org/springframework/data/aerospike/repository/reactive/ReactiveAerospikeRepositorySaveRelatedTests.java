package org.springframework.data.aerospike.repository.reactive;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.sample.CompositeObject;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.ReactiveCompositeObjectRepository;
import org.springframework.data.aerospike.sample.ReactiveCustomerRepository;
import org.springframework.data.aerospike.sample.SimpleObject;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Igor Ermolenko
 */
public class ReactiveAerospikeRepositorySaveRelatedTests extends BaseReactiveIntegrationTests {

    @Autowired
    ReactiveCustomerRepository customerRepo;
    @Autowired
    ReactiveCompositeObjectRepository compositeRepo;

    private Customer customer1, customer2, customer3;

    @BeforeEach
    public void setUp() {
        customer1 = Customer.builder().id(nextId()).firstName("Homer").lastName("Simpson").age(42).build();
        customer2 = Customer.builder().id(nextId()).firstName("Marge").lastName("Simpson").age(39).build();
        customer3 = Customer.builder().id(nextId()).firstName("Bart").lastName("Simpson").age(15).build();
    }

    @Test
    public void saveEntityShouldInsertNewEntity() {
        StepVerifier.create(customerRepo.save(customer1).subscribeOn(Schedulers.parallel())).expectNext(customer1).verifyComplete();

        assertCustomerExistsInRepo(customer1);
    }

    @Test
    public void saveEntityShouldUpdateExistingEntity() {
        StepVerifier.create(customerRepo.save(customer1).subscribeOn(Schedulers.parallel())).expectNext(customer1).verifyComplete();

        customer1.setFirstName("Matt");
        customer1.setLastName("Groening");

        StepVerifier.create(customerRepo.save(customer1).subscribeOn(Schedulers.parallel())).expectNext(customer1).verifyComplete();

        assertCustomerExistsInRepo(customer1);
    }

    @Test
    public void saveIterableOfNewEntitiesShouldInsertEntity() {
        StepVerifier.create(customerRepo.saveAll(Arrays.asList(customer1, customer2, customer3)).subscribeOn(Schedulers.parallel()))
                .recordWith(ArrayList::new)
                .thenConsumeWhile(customer -> true)
                .consumeRecordedWith(actual ->
                        assertThat(actual).containsOnly(customer1, customer2, customer3)
                ).verifyComplete();

        assertCustomerExistsInRepo(customer1);
        assertCustomerExistsInRepo(customer2);
        assertCustomerExistsInRepo(customer3);
    }

    @Test
    public void saveIterableOfMixedEntitiesShouldInsertNewAndUpdateOld() {
        StepVerifier.create(customerRepo.save(customer1).subscribeOn(Schedulers.parallel()))
                .expectNext(customer1).verifyComplete();

        customer1.setFirstName("Matt");
        customer1.setLastName("Groening");

        StepVerifier.create(customerRepo.saveAll(Arrays.asList(customer1, customer2, customer3)).subscribeOn(Schedulers.parallel()))
                .expectNextCount(3).verifyComplete();

        assertCustomerExistsInRepo(customer1);
        assertCustomerExistsInRepo(customer2);
        assertCustomerExistsInRepo(customer3);
    }

    @Test
    public void savePublisherOfEntitiesShouldInsertEntity() {
        StepVerifier.create(customerRepo.saveAll(Flux.just(customer1, customer2, customer3)).subscribeOn(Schedulers.parallel()))
                .expectNextCount(3).verifyComplete();

        assertCustomerExistsInRepo(customer1);
        assertCustomerExistsInRepo(customer2);
        assertCustomerExistsInRepo(customer3);
    }

    @Test
    public void savePublisherOfMixedEntitiesShouldInsertNewAndUpdateOld() {
        StepVerifier.create(customerRepo.save(customer1).subscribeOn(Schedulers.parallel()))
                .expectNext(customer1).verifyComplete();

        customer1.setFirstName("Matt");
        customer1.setLastName("Groening");

        StepVerifier.create(customerRepo.saveAll(Flux.just(customer1, customer2, customer3))).expectNextCount(3).verifyComplete();

        assertCustomerExistsInRepo(customer1);
        assertCustomerExistsInRepo(customer2);
        assertCustomerExistsInRepo(customer3);
    }

    @Test
    public void shouldSaveObjectWithPersistenceConstructorThatHasAllFields() {
        CompositeObject expected = CompositeObject.builder()
                .id("composite-object-1")
                .intValue(15)
                .simpleObject(SimpleObject.builder().property1("prop1").property2(555).build())
                .build();

        StepVerifier.create(compositeRepo.save(expected).subscribeOn(Schedulers.parallel()))
                .expectNext(expected).verifyComplete();

        StepVerifier.create(compositeRepo.findById(expected.getId())).consumeNextWith(actual -> {
            assertThat(actual.getIntValue()).isEqualTo(expected.getIntValue());
            assertThat(actual.getSimpleObject().getProperty1()).isEqualTo(expected.getSimpleObject().getProperty1());
            assertThat(actual.getSimpleObject().getProperty2()).isEqualTo(expected.getSimpleObject().getProperty2());
        }).verifyComplete();
    }

    private void assertCustomerExistsInRepo(Customer customer) {
        StepVerifier.create(customerRepo.findById(customer.getId())).consumeNextWith(actual -> {
            assertThat(actual.getFirstName()).isEqualTo(customer.getFirstName());
            assertThat(actual.getLastName()).isEqualTo(customer.getLastName());
        }).verifyComplete();
    }
}
