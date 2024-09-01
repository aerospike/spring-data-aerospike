package org.springframework.data.aerospike.core.sync;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.PersonMissingAndRedundantFields;
import org.springframework.data.aerospike.sample.PersonSomeFields;
import org.springframework.data.aerospike.sample.PersonTouchOnRead;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class AerospikeTemplateFindByIdProjectionTests extends BaseBlockingIntegrationTests {

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
        template.save(firstPerson);
        template.save(secondPerson);

        PersonSomeFields result = template.findById(firstPerson.getId(), Person.class, PersonSomeFields.class);
        assertThat(result.getFirstName()).isEqualTo("first");
        assertThat(result.getLastName()).isEqualTo("lastName1");
        assertThat(result.getEmailAddress()).isEqualTo("gmail.com");
        template.delete(firstPerson); // cleanup
        template.delete(secondPerson); //cleanup
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
        template.save(firstPerson);
        template.save(secondPerson);

        PersonMissingAndRedundantFields result = template.findById(firstPerson.getId(), Person.class,
            PersonMissingAndRedundantFields.class);

        assertThat(result.getFirstName()).isEqualTo("first");
        assertThat(result.getLastName()).isEqualTo("lastName1");
        assertThat(result.getMissingField()).isNull();
        assertThat(result.getEmailAddress()).isNull(); // Not annotated with @Field("email").
        template.delete(firstPerson); // cleanup
        template.delete(secondPerson); //cleanup
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
        template.save(firstPerson);
        template.save(secondPerson);

        PersonMissingAndRedundantFields result = template.findById(firstPerson.getId(), PersonTouchOnRead.class,
            PersonMissingAndRedundantFields.class);

        assertThat(result.getFirstName()).isEqualTo("first");
        assertThat(result.getLastName()).isEqualTo("lastName1");
        assertThat(result.getMissingField()).isNull();
        assertThat(result.getEmailAddress()).isNull(); // Not annotated with @Field("email").
        template.delete(firstPerson); // cleanup
        template.delete(secondPerson); //cleanup
    }

    @Test
    public void findByIdWithProjectionPersonWithMissingFieldsIncludingTouchOnReadAndSetName() {
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
        template.save(firstPerson, OVERRIDE_SET_NAME);
        template.save(secondPerson, OVERRIDE_SET_NAME);

        PersonMissingAndRedundantFields result = template.findById(firstPerson.getId(), PersonTouchOnRead.class,
            PersonMissingAndRedundantFields.class, OVERRIDE_SET_NAME);

        assertThat(result.getFirstName()).isEqualTo("first");
        assertThat(result.getLastName()).isEqualTo("lastName1");
        assertThat(result.getMissingField()).isNull();
        assertThat(result.getEmailAddress()).isNull(); // Not annotated with @Field("email").
        template.delete(firstPerson, OVERRIDE_SET_NAME); // cleanup
        template.delete(secondPerson, OVERRIDE_SET_NAME); //cleanup
    }

    @Test
    public void findByIdsWithTargetClass_shouldFindExisting() {
        Person firstPerson = Person.builder().id(nextId()).firstName("first").emailAddress("gmail.com").age(40).build();
        Person secondPerson = Person.builder().id(nextId()).firstName("second").emailAddress("gmail.com").age(50)
            .build();
        template.save(firstPerson);
        template.save(secondPerson);

        List<String> ids = Arrays.asList(nextId(), firstPerson.getId(), secondPerson.getId());
        List<PersonSomeFields> actual = template.findByIds(ids, Person.class, PersonSomeFields.class);

        assertThat(actual).containsExactly(firstPerson.toPersonSomeFields(), secondPerson.toPersonSomeFields());
        template.delete(firstPerson); // cleanup
        template.delete(secondPerson); //cleanup
    }

    @Test
    public void findByIdsWithTargetClassAndSetName_shouldFindExisting() {
        Person firstPerson = Person.builder().id(nextId()).firstName("first").emailAddress("gmail.com").age(40).build();
        Person secondPerson = Person.builder().id(nextId()).firstName("second").emailAddress("gmail.com").age(50)
            .build();
        template.save(firstPerson, OVERRIDE_SET_NAME);
        template.save(secondPerson, OVERRIDE_SET_NAME);

        List<String> ids = Arrays.asList(nextId(), firstPerson.getId(), secondPerson.getId());
        List<PersonSomeFields> actual = template.findByIds(ids, Person.class, PersonSomeFields.class, OVERRIDE_SET_NAME);

        assertThat(actual).containsExactly(firstPerson.toPersonSomeFields(), secondPerson.toPersonSomeFields());
        template.delete(firstPerson, OVERRIDE_SET_NAME); // cleanup
        template.delete(secondPerson, OVERRIDE_SET_NAME); //cleanup
    }

    @Test
    public void findByIdsWithTargetClass_shouldReturnEmptyList() {
        List<PersonSomeFields> actual = template.findByIds(Collections.emptyList(), Person.class,
            PersonSomeFields.class);
        assertThat(actual).isEmpty();
    }

    @Test
    public void findByIdsWithTargetClassAndSetName_shouldReturnEmptyList() {
        List<PersonSomeFields> actual = template.findByIds(Collections.emptyList(), Person.class,
            PersonSomeFields.class, OVERRIDE_SET_NAME);
        assertThat(actual).isEmpty();
    }
}
