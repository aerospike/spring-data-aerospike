package org.springframework.data.aerospike.core.reactive;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.PersonMissingAndRedundantFields;
import org.springframework.data.aerospike.sample.PersonSomeFields;
import org.springframework.data.aerospike.sample.PersonTouchOnRead;
import reactor.core.scheduler.Schedulers;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ReactiveAerospikeTemplateFindProjectionTests extends BaseReactiveIntegrationTests {

    @Test
    public void findByIdWithProjection() {
        Person firstPerson = Person.builder()
                .id(nextId())
                .firstName("first")
                .lastName("lastName1")
                .emailAddress("gmail.com")
                .age(40)
                .build();
        Person secondPerson = Person.builder()
                .id(nextId())
                .firstName("second")
                .lastName("lastName2")
                .emailAddress("gmail.com")
                .age(50)
                .build();
        reactiveTemplate.save(firstPerson).subscribeOn(Schedulers.parallel()).block();
        reactiveTemplate.save(secondPerson).subscribeOn(Schedulers.parallel()).block();

        PersonSomeFields result = reactiveTemplate.findById(firstPerson.getId(), Person.class, PersonSomeFields.class)
                .subscribeOn(Schedulers.parallel()).block();

        assert result != null;
        assertThat(result.getFirstName()).isEqualTo("first");
        assertThat(result.getLastName()).isEqualTo("lastName1");
        assertThat(result.getEmailAddress()).isEqualTo("gmail.com");
    }

    @Test
    public void findByIdWithProjectionPersonWithMissingFields() {
        Person firstPerson = Person.builder()
                .id(nextId())
                .firstName("first")
                .lastName("lastName1")
                .emailAddress("gmail.com")
                .build();
        Person secondPerson = Person.builder()
                .id(nextId())
                .firstName("second")
                .lastName("lastName2")
                .emailAddress("gmail.com")
                .build();
        reactiveTemplate.save(firstPerson).subscribeOn(Schedulers.parallel()).block();
        reactiveTemplate.save(secondPerson).subscribeOn(Schedulers.parallel()).block();

        PersonMissingAndRedundantFields result = reactiveTemplate.findById(firstPerson.getId(), Person.class,
                PersonMissingAndRedundantFields.class).subscribeOn(Schedulers.parallel()).block();

        assert result != null;
        assertThat(result.getFirstName()).isEqualTo("first");
        assertThat(result.getLastName()).isEqualTo("lastName1");
        assertThat(result.getMissingField()).isNull();
        assertThat(result.getEmailAddress()).isNull(); // Not annotated with @Field("email").
    }

    @Test
    public void findByIdWithProjectionPersonWithMissingFieldsIncludingTouchOnRead() {
        PersonTouchOnRead firstPerson = PersonTouchOnRead.builder()
                .id(nextId())
                .firstName("first")
                .lastName("lastName1")
                .emailAddress("gmail.com")
                .build();
        PersonTouchOnRead secondPerson = PersonTouchOnRead.builder()
                .id(nextId())
                .firstName("second")
                .lastName("lastName2")
                .emailAddress("gmail.com")
                .build();
        reactiveTemplate.save(firstPerson).subscribeOn(Schedulers.parallel()).block();
        reactiveTemplate.save(secondPerson).subscribeOn(Schedulers.parallel()).block();

        PersonMissingAndRedundantFields result = reactiveTemplate.findById(firstPerson.getId(), PersonTouchOnRead.class,
                PersonMissingAndRedundantFields.class).subscribeOn(Schedulers.parallel()).block();

        assert result != null;
        assertThat(result.getFirstName()).isEqualTo("first");
        assertThat(result.getLastName()).isEqualTo("lastName1");
        assertThat(result.getMissingField()).isNull();
        assertThat(result.getEmailAddress()).isNull(); // Not annotated with @Field("email").
    }

    @Test
    public void findByIdsWithTargetClass_shouldFindExisting() {
        Person firstPerson = Person.builder().id(nextId()).firstName("first").emailAddress("gmail.com").age(40).build();
        Person secondPerson = Person.builder().id(nextId()).firstName("second").emailAddress("gmail.com").age(50).build();
        reactiveTemplate.save(firstPerson).subscribeOn(Schedulers.parallel()).block();
        reactiveTemplate.save(secondPerson).subscribeOn(Schedulers.parallel()).block();

        List<String> ids = Arrays.asList(nextId(), firstPerson.getId(), secondPerson.getId());

        List<PersonSomeFields> actual = reactiveTemplate.findByIds(ids, Person.class, PersonSomeFields.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();

        assertThat(actual).containsExactlyInAnyOrder(
                firstPerson.toPersonSomeFields(),
                secondPerson.toPersonSomeFields());
    }

    @Test
    public void findByIdsWithTargetClass_shouldReturnEmptyList() {
        List<PersonSomeFields> actual = reactiveTemplate.findByIds(Collections.emptyList(), Person.class, PersonSomeFields.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();

        assertThat(actual).isEmpty();
    }
}
