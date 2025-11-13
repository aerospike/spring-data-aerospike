package org.springframework.data.aerospike.core.reactive.delete;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.WritePolicy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.SampleClasses;
import org.springframework.data.aerospike.util.AwaitilityUtils;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.sample.SampleClasses.VersionedClass;

/**
 * Tests for delete related methods in {@link ReactiveAerospikeTemplate}.
 *
 * @author Yevhen Tsyba
 */
public class ReactiveAerospikeTemplateDeleteRelatedTests extends BaseReactiveIntegrationTests {

    @BeforeEach
    public void beforeEach() {
        reactiveTemplate.deleteAll(Person.class).block();
        reactiveTemplate.deleteAll(Customer.class).block();
        reactiveTemplate.deleteAll(VersionedClass.class).block();
        reactiveTemplate.deleteAll(SampleClasses.CollectionOfObjects.class).block();
    }

    @Test
    public void deleteByObject_ignoresVersionEvenIfDefaultGenerationPolicyIsSet() {
        WritePolicy writePolicyDefault = reactorClient.getWritePolicyDefault();
        GenerationPolicy initialGenerationPolicy = writePolicyDefault.generationPolicy;
        writePolicyDefault.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
        try {
            Person initialDocument = new Person(id, "a");
            reactiveTemplate.insert(initialDocument).block();
            reactiveTemplate.update(new Person(id, "b")).block();

            Mono<Boolean> deleted = reactiveTemplate.delete(initialDocument).subscribeOn(Schedulers.parallel());
            StepVerifier.create(deleted).expectNext(true).verifyComplete();
        } finally {
            writePolicyDefault.generationPolicy = initialGenerationPolicy;
        }
    }

    @Test
    public void simpleDeleteById() {
        // given
        Person person = new Person(id, "QLastName", 21);

        Mono<Person> created = reactiveTemplate.insert(person).subscribeOn(Schedulers.parallel());
        StepVerifier.create(created).expectNext(person).verifyComplete();

        // when
        Mono<Boolean> deleted = reactiveTemplate.deleteById(id, Person.class).subscribeOn(Schedulers.parallel());
        StepVerifier.create(deleted).expectNext(true).verifyComplete();

        // then
        Mono<Person> result = reactiveTemplate.findById(id, Person.class).subscribeOn(Schedulers.parallel());
        StepVerifier.create(result).expectComplete().verify();
    }

    @Test
    public void simpleDeleteByObject() {
        // given
        Person person = new Person(id, "QLastName", 21);

        Mono<Person> created = reactiveTemplate.insert(person).subscribeOn(Schedulers.parallel());
        StepVerifier.create(created).expectNext(person).verifyComplete();

        // when
        Mono<Boolean> deleted = reactiveTemplate.delete(person).subscribeOn(Schedulers.parallel());
        StepVerifier.create(deleted).expectNext(true).verifyComplete();

        // then
        Mono<Person> result = reactiveTemplate.findById(id, Person.class).subscribeOn(Schedulers.parallel());
        StepVerifier.create(result).expectComplete().verify();
    }

    @Test
    public void simpleDeleteByObjectWithSetName() {
        // given
        Person person = new Person(id, "QLastName", 21);

        Mono<Person> created = reactiveTemplate.insert(person, OVERRIDE_SET_NAME).subscribeOn(Schedulers.parallel());
        StepVerifier.create(created).expectNext(person).verifyComplete();

        // when
        Mono<Boolean> deleted = reactiveTemplate.delete(person, OVERRIDE_SET_NAME).subscribeOn(Schedulers.parallel());
        StepVerifier.create(deleted).expectNext(true).verifyComplete();

        // then
        Mono<Person> result = reactiveTemplate.findById(id, Person.class, OVERRIDE_SET_NAME)
            .subscribeOn(Schedulers.parallel());
        StepVerifier.create(result).expectComplete().verify();
    }

    @Test
    public void deleteByObject_VersionsMismatch() {
        VersionedClass versionedDocument = new VersionedClass(nextId(), "test");

        reactiveTemplate.insert(versionedDocument).block();
        versionedDocument.setVersion(2);
        assertThatThrownBy(() -> reactiveTemplate.delete(versionedDocument).block())
            .isInstanceOf(OptimisticLockingFailureException.class)
            .hasMessage("Failed to delete record due to versions mismatch");
    }

    @Test
    public void deleteById_shouldReturnFalseIfValueIsAbsent() {
        // when
        Mono<Boolean> deleted = reactiveTemplate.deleteById(id, Person.class).subscribeOn(Schedulers.parallel());
        // then
        StepVerifier.create(deleted).expectComplete().verify();

        assertThat(reactiveTemplate.delete(new Person(id, "QLastName", 21)).block()).isFalse();
        assertThat(reactiveTemplate.delete(new VersionedClass(nextId(), "test")).block()).isFalse();
    }

    @Test
    public void deleteByIdWithSetName_shouldReturnFalseIfValueIsAbsent() {
        // when
        Mono<Boolean> deleted = reactiveTemplate.deleteById(id, OVERRIDE_SET_NAME).subscribeOn(Schedulers.parallel());

        // then
        StepVerifier.create(deleted).expectComplete().verify();
    }

    @Test
    public void deleteByObject_shouldReturnFalseIfValueIsAbsent() {
        // given
        Person person = Person.builder().id(id).firstName("tya").emailAddress("gmail.com").build();

        // when
        Mono<Boolean> deleted = reactiveTemplate.delete(person).subscribeOn(Schedulers.parallel());

        // then
        StepVerifier.create(deleted).expectNext(false);
    }

    @Test
    public void deleteByIds_ShouldDeleteAllDocuments() {
        String id1 = nextId();
        String id2 = nextId();
        reactiveTemplate.save(new SampleClasses.VersionedClass(id1, "test1")).block();
        reactiveTemplate.save(new SampleClasses.VersionedClass(id2, "test2")).block();

        List<String> ids = List.of(id1, id2);
        reactiveTemplate.deleteByIds(ids, SampleClasses.VersionedClass.class).block();

        List<SampleClasses.VersionedClass> list = reactiveTemplate.findByIds(ids,
            SampleClasses.VersionedClass.class).subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(list).isEmpty();

        List<Person> persons = additionalAerospikeTestOperations.saveGeneratedPersons(101);
        ids = persons.stream().map(Person::getId).toList();
        reactiveTemplate.deleteByIds(ids, Person.class).block();
        assertThat(reactiveTemplate.findByIds(ids, Person.class).collectList().block()).hasSize(0);

        List<Person> persons2 = additionalAerospikeTestOperations.saveGeneratedPersons(1001);
        ids = persons2.stream().map(Person::getId).toList();
        reactiveTemplate.deleteByIds(ids, Person.class).block();
        assertThat(reactiveTemplate.findByIds(ids, Person.class).collectList().block()).hasSize(0);
    }

    @Test
    public void deleteByIdsWithSetName_ShouldDeleteAllDocuments() {
        String id1 = nextId();
        String id2 = nextId();
        reactiveTemplate.save(new SampleClasses.DocumentWithExpiration(id1), OVERRIDE_SET_NAME).block();
        reactiveTemplate.save(new SampleClasses.DocumentWithExpiration(id2), OVERRIDE_SET_NAME).block();

        List<String> ids = List.of(id1, id2);
        reactiveTemplate.deleteByIds(ids, OVERRIDE_SET_NAME).block();

        List<SampleClasses.DocumentWithExpiration> list = reactiveTemplate.findByIds(ids,
                SampleClasses.DocumentWithExpiration.class, OVERRIDE_SET_NAME)
            .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(list).isEmpty();
    }

    @Test
    public void deleteByIdsFromDifferentSets_ShouldDeleteAllDocuments() {
        SampleClasses.DocumentWithExpiration entity1_1 = new SampleClasses.DocumentWithExpiration(id);
        SampleClasses.DocumentWithExpiration entity1_2 = new SampleClasses.DocumentWithExpiration(nextId());
        SampleClasses.VersionedClass entity2_1 = new SampleClasses.VersionedClass(nextId(), "test1");
        SampleClasses.VersionedClass entity2_2 = new SampleClasses.VersionedClass(nextId(), "test2");
        Person entity3_1 = Person.builder().id(nextId()).firstName("Name1").build();
        Person entity3_2 = Person.builder().id(nextId()).firstName("Name2").build();
        reactiveTemplate.save(entity1_1).block();
        reactiveTemplate.save(entity1_2).block();
        reactiveTemplate.save(entity2_1).block();
        reactiveTemplate.save(entity2_2).block();
        reactiveTemplate.save(entity3_1).block();
        reactiveTemplate.save(entity3_2).block();

        Map<Class<?>, Collection<?>> entitiesKeys = Map.of(
            SampleClasses.DocumentWithExpiration.class, List.of(entity1_1.getId(), entity1_2.getId()),
            SampleClasses.VersionedClass.class, List.of(entity2_1.getId(), entity2_2.getId()),
            Person.class, List.of(entity3_1.getId(), entity3_2.getId())
        );
        GroupedKeys groupedKeys = GroupedKeys.builder().entitiesKeys(entitiesKeys).build();
        reactiveTemplate.deleteByIds(groupedKeys).block();

        List<SampleClasses.DocumentWithExpiration> list1 = reactiveTemplate.findByIds(
            List.of(entity1_1.getId(), entity1_2.getId()),
            SampleClasses.DocumentWithExpiration.class
        ).subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(list1).isEmpty();
        List<SampleClasses.VersionedClass> list2 = reactiveTemplate.findByIds(
            List.of(entity2_1.getId(), entity2_2.getId()),
            SampleClasses.VersionedClass.class
        ).subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(list2).isEmpty();
        List<Person> list3 = reactiveTemplate.findByIds(
            List.of(entity3_1.getId(), entity3_2.getId()),
            Person.class
        ).subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(list3).isEmpty();
    }

    @Test
    public void deleteExistingByIds_rejectsDuplicateIds() {
        String id1 = nextId();
        SampleClasses.DocumentWithExpiration document1 = new SampleClasses.DocumentWithExpiration(id1);
        SampleClasses.DocumentWithExpiration document2 = new SampleClasses.DocumentWithExpiration(id1);
        reactiveTemplate.save(document1).block();
        reactiveTemplate.save(document2).block();

        List<String> ids = List.of(id1, id1);
        StepVerifier.create(reactiveTemplate.deleteExistingByIds(ids, SampleClasses.DocumentWithExpiration.class))
            .expectError(AerospikeException.BatchRecordArray.class)
            .verify();
    }

    @Test
    public void deleteByIds_skipsDuplicateIds() {
        List<Person> persons = additionalAerospikeTestOperations.saveGeneratedPersons(201);
        reactiveTemplate.deleteAll(persons).block();
        var ids = persons.stream().map(Person::getId).toList();
        assertThat(reactiveTemplate.findByIds(ids, Person.class).collectList().block()).hasSize(0);

        // Ignores non-existing records
        StepVerifier.create(reactiveTemplate.deleteByIds(ids, Person.class))
            .expectComplete()
            .verify();
    }

    @Test
    public void deleteAll_ShouldDeleteAllDocuments() {
        String id1 = nextId();
        String id2 = nextId();
        SampleClasses.DocumentWithExpiration document1 = new SampleClasses.DocumentWithExpiration(id1);
        SampleClasses.DocumentWithExpiration document2 = new SampleClasses.DocumentWithExpiration(id2);
        reactiveTemplate.saveAll(List.of(document1, document2)).blockLast();

        List<String> ids = List.of(id1, id2);
        reactiveTemplate.deleteAll(List.of(document1, document2)).block();

        List<SampleClasses.VersionedClass> list = reactiveTemplate.findByIds(ids,
            SampleClasses.VersionedClass.class).subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(list).isEmpty();

        List<Person> persons = additionalAerospikeTestOperations.saveGeneratedPersons(101);
        ids = persons.stream().map(Person::getId).toList();
        reactiveTemplate.deleteAll(persons).block();
        assertThat(reactiveTemplate.findByIds(ids, Person.class).collectList().block()).hasSize(0);

        List<Person> persons2 = additionalAerospikeTestOperations.saveGeneratedPersons(1001);
        ids = persons2.stream().map(Person::getId).toList();
        reactiveTemplate.deleteAll(persons2).block();
        assertThat(reactiveTemplate.findByIds(ids, Person.class).collectList().block()).hasSize(0);
    }

    @Test
    public void deleteAllWithSetName_ShouldDeleteAllDocuments() {
        String id1 = nextId();
        String id2 = nextId();
        SampleClasses.DocumentWithExpiration document1 = new SampleClasses.DocumentWithExpiration(id1);
        SampleClasses.DocumentWithExpiration document2 = new SampleClasses.DocumentWithExpiration(id2);
        reactiveTemplate.saveAll(List.of(document1, document2), OVERRIDE_SET_NAME).blockLast();

        reactiveTemplate.deleteAll(List.of(document1, document2), OVERRIDE_SET_NAME).block();
        List<String> ids = List.of(id1, id2);
        List<SampleClasses.DocumentWithExpiration> list = reactiveTemplate.findByIds(ids,
                SampleClasses.DocumentWithExpiration.class, OVERRIDE_SET_NAME)
            .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(list).isEmpty();
    }

    @Test
    public void deleteAll_ShouldDeleteAllDocumentsBeforeGivenLastUpdateTime() {
        String id1 = nextId();
        String id2 = nextId();
        SampleClasses.CollectionOfObjects document1 = new SampleClasses.CollectionOfObjects(id1, List.of("test1"));
        SampleClasses.CollectionOfObjects document2 = new SampleClasses.CollectionOfObjects(id2, List.of("test2"));

        reactiveTemplate.save(document1).block();
        AwaitilityUtils.wait(1, MILLISECONDS);

        Instant lastUpdateTime = Instant.now();
        Instant inFuture = Instant.ofEpochMilli(lastUpdateTime.toEpochMilli() + 10000);
        reactiveTemplate.save(document2).block();

        // make sure document1 has lastUpdateTime less than specified millis
        List<SampleClasses.CollectionOfObjects> resultsWithLutLtMillis =
            runLastUpdateTimeQuery(lastUpdateTime.toEpochMilli(), FilterOperation.LT,
                SampleClasses.CollectionOfObjects.class);
        assertThat(resultsWithLutLtMillis.get(0).getId()).isEqualTo(document1.getId());
        assertThat(resultsWithLutLtMillis.get(0).getCollection().iterator().next())
            .isEqualTo(document1.getCollection().iterator().next());

        assertThatThrownBy(() ->
            reactiveTemplate.deleteAll(SampleClasses.CollectionOfObjects.class, inFuture).block())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageMatching("Last update time (.*) must be less than the current time");

        reactiveTemplate.deleteAll(SampleClasses.CollectionOfObjects.class, lastUpdateTime).block();
        assertThat(reactiveTemplate.findByIds(List.of(id1, id2), SampleClasses.CollectionOfObjects.class)
            .collectList().block()).hasSize(1);
        SampleClasses.CollectionOfObjects result = reactiveTemplate.findByIds(List.of(id1, id2),
            SampleClasses.CollectionOfObjects.class).collectList().block().get(0);
        assertThat(result.getId()).isEqualTo(document2.getId());
        assertThat(result.getCollection().iterator().next()).isEqualTo(document2.getCollection().iterator().next());

        List<Person> persons = additionalAerospikeTestOperations.saveGeneratedPersons(101);
        AwaitilityUtils.wait(1, MILLISECONDS);
        lastUpdateTime = Instant.now();
        Person newPerson = new Person(nextId(), "testFirstName");
        reactiveTemplate.save(newPerson).block();
        persons.add(newPerson);

        reactiveTemplate.deleteAll(reactiveTemplate.getSetName(Person.class), lastUpdateTime).block();
        List<String> personsIds = persons.stream().map(Person::getId).toList();
        assertThat(reactiveTemplate.findByIds(personsIds, Person.class).collectList().block()).contains(newPerson);

        List<Person> persons2 = additionalAerospikeTestOperations.saveGeneratedPersons(1001);
        reactiveTemplate.deleteAll(Person.class, lastUpdateTime)
            .block(); // persons2 were saved after the given time
        personsIds = persons2.stream().map(Person::getId).toList();
        assertThat(reactiveTemplate.findByIds(personsIds, Person.class).collectList().block())
            .containsExactlyInAnyOrderElementsOf(persons2);
    }

    @Test
    public void deleteAll_rejectsDuplicateIds() {
        String id1 = nextId();
        SampleClasses.DocumentWithExpiration document1 = new SampleClasses.DocumentWithExpiration(id1);
        SampleClasses.DocumentWithExpiration document2 = new SampleClasses.DocumentWithExpiration(id1);
        reactiveTemplate.saveAll(List.of(document1, document2)).blockLast();

        StepVerifier.create(reactiveTemplate.deleteAll(List.of(document1, document2)))
            .expectError(AerospikeException.BatchRecordArray.class)
            .verify();
    }

    @Test
    public void deleteAll_VersionsMismatch() {
        String id1 = "id1";
        VersionedClass document1 = new VersionedClass(id1, "test1");
        String id2 = "id2";
        VersionedClass document2 = new VersionedClass(id2, "test2");
        reactiveTemplate.save(document1).block();
        reactiveTemplate.save(document2).block();

        document2.setVersion(232);
        assertThatThrownBy(() -> reactiveTemplate.deleteAll(List.of(document1, document2)).block())
            .isInstanceOf(OptimisticLockingFailureException.class)
            .hasMessageContaining("Failed to delete the record with ID 'id2' due to versions mismatch");
    }
}
