package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.policy.GenerationPolicy;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.aerospike.SampleClasses;
import org.springframework.data.aerospike.core.Person;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Tests for delete related methods in {@link ReactiveAerospikeTemplate}.
 *
 * @author Yevhen Tsyba
 */
public class ReactiveAerospikeTemplateDeleteRelatedTests extends BaseReactiveAerospikeTemplateTests {
    private String id;

    @Before
    public void setUp() {
        this.id = nextId();
    }

    @Test
    public void deleteByObject_ignoresDocumentVersionEvenIfDefaultGenerationPolicyIsSet() {
        GenerationPolicy initialGenerationPolicy = client.writePolicyDefault.generationPolicy;
        client.writePolicyDefault.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
        try {
            SampleClasses.VersionedClass initialDocument = new SampleClasses.VersionedClass(id, "a");
            reactiveTemplate.insert(initialDocument).block();
            reactiveTemplate.update(new SampleClasses.VersionedClass(id, "b", initialDocument.version)).block();

            Mono<Boolean> deleted = reactiveTemplate.delete(initialDocument);
            StepVerifier.create(deleted).expectNext(true).verifyComplete();
        } finally {
            client.writePolicyDefault.generationPolicy = initialGenerationPolicy;
        }
    }

    @Test
    public void deleteByObject_ignoresVersionEvenIfDefaultGenerationPolicyIsSet() {
        GenerationPolicy initialGenerationPolicy = client.writePolicyDefault.generationPolicy;
        client.writePolicyDefault.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
        try {
            Person initialDocument = new Person(id, "a");
            reactiveTemplate.insert(initialDocument).block();
            reactiveTemplate.update(new Person(id, "b")).block();

            Mono<Boolean> deleted = reactiveTemplate.delete(initialDocument);
            StepVerifier.create(deleted).expectNext(true).verifyComplete();
        } finally {
            client.writePolicyDefault.generationPolicy = initialGenerationPolicy;
        }
    }

    @Test
    public void testSimpleDeleteById() {
        // given
        Person person = new Person(id, "QLastName", 21);

        Mono<Person> created = reactiveTemplate.insert(person);
        StepVerifier.create(created).expectNext(person).verifyComplete();

        // when
        Mono<Boolean> deleted = reactiveTemplate.delete(id, Person.class);
        StepVerifier.create(deleted).expectNext(true).verifyComplete();

        // then
        Mono<Person> result = reactiveTemplate.findById(id, Person.class);
        StepVerifier.create(result).expectComplete().verify();;
    }

    @Test
    public void testSimpleDeleteByObject() {
        // given
        Person person = new Person(id, "QLastName", 21);

        Mono<Person> created = reactiveTemplate.insert(person);
        StepVerifier.create(created).expectNext(person).verifyComplete();

        // when
        Mono<Boolean> deleted = reactiveTemplate.delete(person);
        StepVerifier.create(deleted).expectNext(true).verifyComplete();

        // then
        Mono<Person> result = reactiveTemplate.findById(id, Person.class);
        StepVerifier.create(result).expectComplete().verify();
    }

    @Test
    public void deleteById_shouldReturnFalseIfValueIsAbsent() {
        // when
        Mono<Boolean> deleted = reactiveTemplate.delete(id, Person.class);

        // then
        StepVerifier.create(deleted).expectComplete().verify();
    }

    @Test
    public void deleteByObject_shouldReturnFalseIfValueIsAbsent() {
        // given
        Person person = Person.builder().id(id).firstName("tya").emailAddress("gmail.com").build();

        // when
        Mono<Boolean> deleted = reactiveTemplate.delete(person);

        // then
        StepVerifier.create(deleted).expectComplete().verify();
    }
}
