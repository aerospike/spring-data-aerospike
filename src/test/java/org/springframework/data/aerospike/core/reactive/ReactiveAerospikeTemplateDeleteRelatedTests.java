package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.WritePolicy;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.SampleClasses;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.utility.ServerVersionUtils;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.SampleClasses.VersionedClass;

/**
 * Tests for delete related methods in {@link ReactiveAerospikeTemplate}.
 *
 * @author Yevhen Tsyba
 */
public class ReactiveAerospikeTemplateDeleteRelatedTests extends BaseReactiveIntegrationTests {

    @Test
    public void deleteByObject_ignoresDocumentVersionEvenIfDefaultGenerationPolicyIsSet() {
        WritePolicy writePolicyDefault = reactorClient.getWritePolicyDefault();
        GenerationPolicy initialGenerationPolicy = writePolicyDefault.generationPolicy;
        writePolicyDefault.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
        try {
            VersionedClass initialDocument = new VersionedClass(id, "a");
            reactiveTemplate.insert(initialDocument).block();
            reactiveTemplate.update(new VersionedClass(id, "b", initialDocument.version)).block();

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
        Mono<Person> result = reactiveTemplate.findById(id, Person.class, OVERRIDE_SET_NAME).subscribeOn(Schedulers.parallel());
        StepVerifier.create(result).expectComplete().verify();
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
    public void deleteAll_ShouldDeleteAllDocuments() {
        // batch delete operations are supported starting with Server version 6.0+
        if (ServerVersionUtils.isBatchWriteSupported(reactorClient.getAerospikeClient())) {
            String id1 = nextId();
            String id2 = nextId();
            reactiveTemplate.save(new SampleClasses.DocumentWithExpiration(id1));
            reactiveTemplate.save(new SampleClasses.DocumentWithExpiration(id2));

            List<String> ids = List.of(id1, id2);
            reactiveTemplate.deleteByIds(ids, SampleClasses.DocumentWithExpiration.class);

            List<SampleClasses.DocumentWithExpiration> list = reactiveTemplate.findByIds(ids,
                SampleClasses.DocumentWithExpiration.class).subscribeOn(Schedulers.parallel()).collectList().block();
            assertThat(list).isEmpty();
        }
    }

    @Test
    public void deleteAllWithSetName_ShouldDeleteAllDocuments() {
        // batch delete operations are supported starting with Server version 6.0+
        if (ServerVersionUtils.isBatchWriteSupported(reactorClient.getAerospikeClient())) {
            String id1 = nextId();
            String id2 = nextId();
            reactiveTemplate.save(new SampleClasses.DocumentWithExpiration(id1), OVERRIDE_SET_NAME);
            reactiveTemplate.save(new SampleClasses.DocumentWithExpiration(id2), OVERRIDE_SET_NAME);

            List<String> ids = List.of(id1, id2);
            reactiveTemplate.deleteByIds(ids, OVERRIDE_SET_NAME);

            List<SampleClasses.DocumentWithExpiration> list = reactiveTemplate.findByIds(ids,
                SampleClasses.DocumentWithExpiration.class, OVERRIDE_SET_NAME)
                .subscribeOn(Schedulers.parallel()).collectList().block();
            assertThat(list).isEmpty();
        }
    }

    @Test
    public void deleteAllFromDifferentSets_ShouldDeleteAllDocuments() {
        // batch delete operations are supported starting with Server version 6.0+
        if (ServerVersionUtils.isBatchWriteSupported(reactorClient.getAerospikeClient())) {
            SampleClasses.DocumentWithExpiration entity1 = new SampleClasses.DocumentWithExpiration(id);
            SampleClasses.VersionedClass entity2 = new SampleClasses.VersionedClass(nextId(), "test");
            reactiveTemplate.save(entity1);
            reactiveTemplate.save(entity2);

            reactiveTemplate.deleteByIds(GroupedKeys.builder()
                .entityKeys(SampleClasses.DocumentWithExpiration.class, List.of(entity1.getId())).build());
            reactiveTemplate.deleteByIds(GroupedKeys.builder()
                .entityKeys(SampleClasses.VersionedClass.class, List.of(entity2.getId())).build());

            List<SampleClasses.DocumentWithExpiration> list1 = reactiveTemplate.findByIds(List.of(entity1.getId()),
                SampleClasses.DocumentWithExpiration.class).subscribeOn(Schedulers.parallel()).collectList().block();
            assertThat(list1).isEmpty();
            List<SampleClasses.VersionedClass> list2 = reactiveTemplate.findByIds(List.of(entity2.getId()),
                SampleClasses.VersionedClass.class).subscribeOn(Schedulers.parallel()).collectList().block();
            assertThat(list2).isEmpty();
        }
    }

    @Test
    public void deleteAll_rejectsDuplicateIds() {
        // batch write operations are supported starting with Server version 6.0+
        if (ServerVersionUtils.isBatchWriteSupported(reactorClient.getAerospikeClient())) {
            String id1 = nextId();
            SampleClasses.DocumentWithExpiration document1 = new SampleClasses.DocumentWithExpiration(id1);
            SampleClasses.DocumentWithExpiration document2 = new SampleClasses.DocumentWithExpiration(id1);
            reactiveTemplate.save(document1);
            reactiveTemplate.save(document2);

            List<String> ids = List.of(id1, id1);
            StepVerifier.create(reactiveTemplate.deleteByIds(ids, SampleClasses.DocumentWithExpiration.class))
                .expectError(AerospikeException.BatchRecordArray.class)
                .verify();
        }
    }
}
