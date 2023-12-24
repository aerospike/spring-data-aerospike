package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.WritePolicy;
import org.junit.jupiter.api.Test;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.SampleClasses;
import org.springframework.test.context.TestPropertySource;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.query.cache.IndexRefresher.INDEX_CACHE_REFRESH_SECONDS;
import static org.springframework.data.aerospike.sample.SampleClasses.VersionedClass;

/**
 * Tests for delete related methods in {@link ReactiveAerospikeTemplate}.
 *
 * @author Yevhen Tsyba
 */
@TestPropertySource(properties = {INDEX_CACHE_REFRESH_SECONDS + " = 0", "createIndexesOnStartup = false"})
// this test class does not require secondary indexes created on startup
public class ReactiveAerospikeTemplateDeleteRelatedTests extends BaseReactiveIntegrationTests {

    @Test
    public void deleteByObject_ignoresDocumentVersionEvenIfDefaultGenerationPolicyIsSet() {
        WritePolicy writePolicyDefault = reactorClient.getWritePolicyDefault();
        GenerationPolicy initialGenerationPolicy = writePolicyDefault.generationPolicy;
        writePolicyDefault.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
        try {
            VersionedClass initialDocument = new VersionedClass(id, "a");
            reactiveTemplate.insert(initialDocument).block();
            reactiveTemplate.update(new VersionedClass(id, "b", initialDocument.getVersion())).block();

            Mono<Boolean> deleted = reactiveTemplate.delete(initialDocument).subscribeOn(Schedulers.parallel());
            StepVerifier.create(deleted).expectNext(true).verifyComplete();
        } finally {
            writePolicyDefault.generationPolicy = initialGenerationPolicy;
        }
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
        Person person = new Person(id, "QLastName", 21);
        VersionedClass versionedDocument = new VersionedClass(nextId(), "test");

        assertThat(reactiveTemplate.delete(person).block()).isFalse();
        assertThat(reactiveTemplate.delete(versionedDocument).block()).isFalse();

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
        StepVerifier.create(deleted).expectComplete().verify();
    }

    @Test
    public void deleteByIds_ShouldDeleteAllDocuments() {
        // batch delete operations are supported starting with Server version 6.0+
        if (serverVersionSupport.batchWrite()) {
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
    }

    @Test
    public void deleteByIdsWithSetName_ShouldDeleteAllDocuments() {
        // batch delete operations are supported starting with Server version 6.0+
        if (serverVersionSupport.batchWrite()) {
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
    }

    @Test
    public void deleteByIdsFromDifferentSets_ShouldDeleteAllDocuments() {
        // batch delete operations are supported starting with Server version 6.0+
        if (serverVersionSupport.batchWrite()) {
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
    }

    @Test
    public void deleteByIds_rejectsDuplicateIds() {
        // batch write operations are supported starting with Server version 6.0+
        if (serverVersionSupport.batchWrite()) {
            String id1 = nextId();
            SampleClasses.DocumentWithExpiration document1 = new SampleClasses.DocumentWithExpiration(id1);
            SampleClasses.DocumentWithExpiration document2 = new SampleClasses.DocumentWithExpiration(id1);
            reactiveTemplate.save(document1).block();
            reactiveTemplate.save(document2).block();

            List<String> ids = List.of(id1, id1);
            StepVerifier.create(reactiveTemplate.deleteByIds(ids, SampleClasses.DocumentWithExpiration.class))
                .expectError(AerospikeException.BatchRecordArray.class)
                .verify();
        }
    }

    @Test
    public void deleteAll_ShouldDeleteAllDocuments() {
        // batch delete operations are supported starting with Server version 6.0+
        if (serverVersionSupport.batchWrite()) {
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
    }

    @Test
    public void deleteAllWithSetName_ShouldDeleteAllDocuments() {
        // batch delete operations are supported starting with Server version 6.0+
        if (serverVersionSupport.batchWrite()) {
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
    }

    @Test
    public void deleteAll_rejectsDuplicateIds() {
        // batch write operations are supported starting with Server version 6.0+
        if (serverVersionSupport.batchWrite()) {
            String id1 = nextId();
            SampleClasses.DocumentWithExpiration document1 = new SampleClasses.DocumentWithExpiration(id1);
            SampleClasses.DocumentWithExpiration document2 = new SampleClasses.DocumentWithExpiration(id1);
            reactiveTemplate.saveAll(List.of(document1, document2)).blockLast();

            StepVerifier.create(reactiveTemplate.deleteAll(List.of(document1, document2)))
                .expectError(AerospikeException.BatchRecordArray.class)
                .verify();
        }
    }

    @Test
    public void deleteAll_VersionsMismatch() {
        // batch delete operations are supported starting with Server version 6.0+
        if (serverVersionSupport.batchWrite()) {
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
}
