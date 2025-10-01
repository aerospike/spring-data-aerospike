package org.springframework.data.aerospike.repository.query.reactive.noindex;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.sample.CompositeObject;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.ReactiveCompositeObjectRepository;
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
public class ReactiveAerospikeRepositorySaveRelatedTests extends ReactiveCustomerRepositoryQueryTests {
    
    @Autowired
    ReactiveCompositeObjectRepository compositeRepo;

    @Test
    public void saveEntityShouldInsertNewEntity() {
        StepVerifier.create(reactiveRepository.save(matt).subscribeOn(Schedulers.parallel())).expectNext(matt)
            .verifyComplete();

        assertCustomerExistsInRepo(matt);
    }

    @Test
    public void saveEntityShouldUpdateExistingEntity() {
        StepVerifier.create(reactiveRepository.save(matt).subscribeOn(Schedulers.parallel())).expectNext(matt)
            .verifyComplete();

        matt.setFirstName("Matt");
        matt.setLastName("Groening");

        StepVerifier.create(reactiveRepository.save(matt).subscribeOn(Schedulers.parallel())).expectNext(matt)
            .verifyComplete();

        assertCustomerExistsInRepo(matt);
    }

    @Test
    public void saveIterableOfNewEntitiesShouldInsertEntity() {
        StepVerifier.create(reactiveRepository.saveAll(Arrays.asList(matt, marge, maggie))
                .subscribeOn(Schedulers.parallel()))
            .recordWith(ArrayList::new)
            .thenConsumeWhile(customer -> true)
            .consumeRecordedWith(actual ->
                assertThat(actual).containsOnly(matt, marge, maggie)
            ).verifyComplete();

        assertCustomerExistsInRepo(matt);
        assertCustomerExistsInRepo(marge);
        assertCustomerExistsInRepo(maggie);
    }

    @Test
    public void saveIterableOfMixedEntitiesShouldInsertNewAndUpdateOld() {
        StepVerifier.create(reactiveRepository.save(matt).subscribeOn(Schedulers.parallel()))
            .expectNext(matt).verifyComplete();

        matt.setFirstName("Matt");
        matt.setLastName("Groening");

        StepVerifier.create(reactiveRepository.saveAll(Arrays.asList(matt, marge, maggie))
                .subscribeOn(Schedulers.parallel()))
            .expectNextCount(3).verifyComplete();

        assertCustomerExistsInRepo(matt);
        assertCustomerExistsInRepo(marge);
        assertCustomerExistsInRepo(maggie);
    }

    @Test
    public void savePublisherOfEntitiesShouldInsertEntity() {
        StepVerifier.create(reactiveRepository.saveAll(Flux.just(matt, marge, maggie))
                .subscribeOn(Schedulers.parallel()))
            .expectNextCount(3).verifyComplete();

        assertCustomerExistsInRepo(matt);
        assertCustomerExistsInRepo(marge);
        assertCustomerExistsInRepo(maggie);
    }

    @Test
    public void savePublisherOfMixedEntitiesShouldInsertNewAndUpdateOld() {
        StepVerifier.create(reactiveRepository.save(matt).subscribeOn(Schedulers.parallel()))
            .expectNext(matt).verifyComplete();

        matt.setFirstName("Matt");
        matt.setLastName("Groening");

        StepVerifier.create(reactiveRepository.saveAll(Flux.just(matt, marge, maggie))).expectNextCount(3)
            .verifyComplete();

        assertCustomerExistsInRepo(matt);
        assertCustomerExistsInRepo(marge);
        assertCustomerExistsInRepo(maggie);
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
        StepVerifier.create(reactiveRepository.findById(customer.getId())).consumeNextWith(actual -> {
            assertThat(actual.getFirstName()).isEqualTo(customer.getFirstName());
            assertThat(actual.getLastName()).isEqualTo(customer.getLastName());
        }).verifyComplete();
    }
}
