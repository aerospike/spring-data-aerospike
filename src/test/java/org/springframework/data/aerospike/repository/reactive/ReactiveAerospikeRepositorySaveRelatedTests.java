package org.springframework.data.aerospike.repository.reactive;


import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.sample.*;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Arrays;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;


/**
 * @author Igor Ermolenko
 */
public class ReactiveAerospikeRepositorySaveRelatedTests extends BaseIntegrationTests {

    @Autowired
    ReactiveCustomerRepository customerRepo;
    @Autowired
    ReactiveCompositeObjectRepository compositeRepo;

    private Customer customer1, customer2, customer3;

    @Before
    public void setUp() {
        customer1 = new Customer("Homer", "Simpson");
        customer2 = new Customer("Marge", "Simpson");
        customer3 = new Customer("Bart", "Simpson");
    }


    @Test
    public void saveEntityShouldInsertNewEntity() {
        StepVerifier.create(customerRepo.save(customer1)).expectNext(customer1).verifyComplete();

        assertCustomerExistsInRepo(customer1);
    }

    @Test
    public void saveEntityShouldUpdateExistingEntity() {
        StepVerifier.create(customerRepo.save(customer1)).expectNext(customer1).verifyComplete();

        customer1.setFirstname("Matt");
        customer1.setLastname("Groening");

        StepVerifier.create(customerRepo.save(customer1)).expectNext(customer1).verifyComplete();

        assertCustomerExistsInRepo(customer1);
    }

    @Test
    public void saveIterableOfNewEntitiesShouldInsertEntity() {
        StepVerifier.create(customerRepo.saveAll(Arrays.asList(customer1, customer2, customer3)))
                .recordWith(ArrayList::new)
                .thenConsumeWhile(customer -> true)
                .consumeRecordedWith(actual ->
                        assertThat(actual, containsInAnyOrder(customer1, customer2, customer3))
                ).verifyComplete();

        assertCustomerExistsInRepo(customer1);
        assertCustomerExistsInRepo(customer2);
        assertCustomerExistsInRepo(customer3);
    }

    @Test
    public void saveIterableOfMixedEntitiesShouldInsertNewAndUpdateOld() {
        StepVerifier.create(customerRepo.save(customer1)).expectNext(customer1).verifyComplete();

        customer1.setFirstname("Matt");
        customer1.setLastname("Groening");

        StepVerifier.create(customerRepo.saveAll(Arrays.asList(customer1, customer2, customer3))).expectNextCount(3).verifyComplete();

        assertCustomerExistsInRepo(customer1);
        assertCustomerExistsInRepo(customer2);
        assertCustomerExistsInRepo(customer3);
    }

    @Test
    public void savePublisherOfEntitiesShouldInsertEntity() {
        StepVerifier.create(customerRepo.saveAll(Flux.just(customer1, customer2, customer3))).expectNextCount(3).verifyComplete();

        assertCustomerExistsInRepo(customer1);
        assertCustomerExistsInRepo(customer2);
        assertCustomerExistsInRepo(customer3);
    }

    @Test
    public void savePublisherOfMixedEntitiesShouldInsertNewAndUpdateOld() {
        StepVerifier.create(customerRepo.save(customer1)).expectNext(customer1).verifyComplete();

        customer1.setFirstname("Matt");
        customer1.setLastname("Groening");

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

        StepVerifier.create(compositeRepo.save(expected)).expectNext(expected).verifyComplete();

        StepVerifier.create(compositeRepo.findById(expected.getId())).consumeNextWith(actual -> {
            assertThat(actual.getIntValue(), is(equalTo(expected.getIntValue())));
            assertThat(actual.getSimpleObject().getProperty1(), is(equalTo(expected.getSimpleObject().getProperty1())));
            assertThat(actual.getSimpleObject().getProperty2(), is(equalTo(expected.getSimpleObject().getProperty2())));
        }).verifyComplete();
    }


    private void assertCustomerExistsInRepo(Customer customer) {
        StepVerifier.create(customerRepo.findById(customer.getId())).consumeNextWith(actual -> {
            assertThat(actual.getFirstname(), is(equalTo(customer.getFirstname())));
            assertThat(actual.getLastname(), is(equalTo(customer.getLastname())));
        }).verifyComplete();
    }

}
