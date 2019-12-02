package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.query.IndexType;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.data.aerospike.SampleClasses;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.domain.Sort;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.springframework.data.aerospike.SampleClasses.EXPIRATION_ONE_MINUTE;

/**
 * Tests for find related methods in {@link ReactiveAerospikeTemplate}.
 *
 * @author Igor Ermolenko
 */
public class ReactiveAerospikeTemplateFindRelatedTests extends BaseReactiveAerospikeTemplateTests {
    @Test
    public void findById_shouldReturnValueForExistingKey() {
        Person person = new Person(id, "Dave", "Matthews");
        StepVerifier.create(reactiveTemplate.save(person)).expectNext(person).verifyComplete();

        StepVerifier.create(reactiveTemplate.findById(id, Person.class)).consumeNextWith(actual -> {
            Assert.assertThat(actual.getFirstname(), is(equalTo(person.getFirstname())));
            Assert.assertThat(actual.getLastname(), is(equalTo(person.getLastname())));
        }).verifyComplete();
    }

    @Test
    public void findById_shouldReturnNullForNonExistingKey() {
        StepVerifier.create(reactiveTemplate.findById("dave-is-absent", Person.class))
                .expectNextCount(0).verifyComplete();
    }

    @Test
    public void findById_shouldReturnNullForNonExistingKeyIfTouchOnReadSetToTrue() {
        StepVerifier.create(reactiveTemplate.findById("foo-is-absent", SampleClasses.DocumentWithTouchOnRead.class))
                .expectNextCount(0).verifyComplete();

    }

    @Test
    public void findById_shouldIncreaseVersionIfTouchOnReadSetToTrue() {
        SampleClasses.DocumentWithTouchOnRead document = new SampleClasses.DocumentWithTouchOnRead(id, 1);
        StepVerifier.create(reactiveTemplate.save(document)).expectNext(document).verifyComplete();

        StepVerifier.create(reactiveTemplate.findById(document.getId(), SampleClasses.DocumentWithTouchOnRead.class)).consumeNextWith(actual -> {
            Assert.assertThat(actual.getVersion(), is(equalTo(document.getVersion() + 1)));
        }).verifyComplete();
    }

    @Test(expected = IllegalStateException.class)
    public void findById_shouldFailOnTouchOnReadWithExpirationProperty() {
        SampleClasses.DocumentWithTouchOnReadAndExpirationProperty document = new SampleClasses.DocumentWithTouchOnReadAndExpirationProperty(id, EXPIRATION_ONE_MINUTE);
        reactiveTemplate.insert(document).block();
        reactiveTemplate.findById(document.getId(), SampleClasses.DocumentWithTouchOnReadAndExpirationProperty.class);
    }

    @Test
    public void findAll_findsAllExistingDocuments() {
        List<Person> persons = IntStream.rangeClosed(1, 10)
                .mapToObj(age -> new Person(nextId(), "Dave", "Matthews", age))
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        List<Person> result = reactiveTemplate.findAll(Person.class).collectList().block();
        assertThat(result).containsOnlyElementsOf(persons);
    }

    @Test
    public void findAll_findsNothing() throws Exception {
        List<Person> result = reactiveTemplate.findAll(Person.class).collectList().block();

        assertThat(result).isEmpty();
    }

    @Test
    public void findByIds_shouldReturnEmptyList() {
        Long userCount = reactiveTemplate.findByIds(Collections.emptyList(), Person.class).count().block();
        assertThat(userCount).isEqualTo(0);
    }

    @Test
    public void findByIds_shouldFindExisting() {
        Person customer1 = new Person(nextId(), "Dave", "Matthews");
        Person customer2 = new Person(nextId(), "James", "Bond");
        Person customer3 = new Person(nextId(), "Matt", "Groening");
        reactiveTemplate.insertAll(Arrays.asList(customer1, customer2, customer3)).blockLast();

        List<String> ids = Arrays.asList("unknown", customer1.getId(), customer2.getId());
        List<Person> actual = reactiveTemplate.findByIds(ids, Person.class).collectList().block();

        assertThat(actual).containsExactlyInAnyOrder(customer1, customer2);
    }

    @Test
    public void findInRange_shouldFindLimitedNumberOfDocuments() {
        List<Person> allUsers = IntStream.range(20, 27)
                .mapToObj(id -> new Person(nextId(), "Firstname", "Lastname")).collect(Collectors.toList());
        reactiveTemplate.insertAll(allUsers).blockLast();

        List<Person> actual = reactiveTemplate.findInRange(0, 5, Sort.unsorted(), Person.class).collectList().block();
        assertThat(actual)
                .hasSize(5)
                .containsAnyElementsOf(allUsers);
    }

    @Test
    public void findInRange_shouldFindLimitedNumberOfDocumentsAndSkip() {
        List<Person> allUsers = IntStream.range(20, 27)
                .mapToObj(id -> new Person(nextId(), "Firstname", "Lastname")).collect(Collectors.toList());
        reactiveTemplate.insertAll(allUsers).blockLast();

        List<Person> actual = reactiveTemplate.findInRange(0, 5, Sort.unsorted(), Person.class).collectList().block();

        assertThat(actual)
                .hasSize(5)
                .containsAnyElementsOf(allUsers);
    }

    @Test
    public void find_throwsExceptionForUnsortedQueryWithSpecifiedOffsetValue() {
        Query query = new Query((Sort) null);
        query.setOffset(1);

        assertThatThrownBy(() -> reactiveTemplate.find(query, Person.class).collectList().block())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsorted query must not have offset value. For retrieving paged results use sorted query.");
    }


    @Test
    public void find_shouldWorkWithFilterEqual() {
        createIndexIfNotExists(Person.class, "first_name_index", "firstname", IndexType.STRING);
        List<Person> allUsers = IntStream.rangeClosed(1, 10)
                .mapToObj(id -> new Person(nextId(), "Dave", "Matthews")).collect(Collectors.toList());
        reactiveTemplate.insertAll(allUsers).blockLast();

        Query query = createQueryForMethodWithArgs("findPersonByFirstname", "Dave");

        List<Person> actual = reactiveTemplate.find(query, Person.class).collectList().block();
        assertThat(actual)
                .hasSize(10)
                .containsExactlyInAnyOrderElementsOf(allUsers);
    }

    @Test
    public void find_shouldWorkWithFilterEqualOrderBy() {
        createIndexIfNotExists(Person.class, "age_index", "age", IndexType.NUMERIC);
        createIndexIfNotExists(Person.class, "last_name_index", "lastname", IndexType.STRING);

        List<Person> allUsers = IntStream.rangeClosed(1, 10)
                .mapToObj(id -> new Person(nextId(), "Dave" + id, "Matthews")).collect(Collectors.toList());
        Collections.shuffle(allUsers); // Shuffle user list
        reactiveTemplate.insertAll(allUsers).blockLast();
        allUsers.sort(Comparator.comparing(Person::getFirstname)); // Order user list by firstname ascending

        Query query = createQueryForMethodWithArgs("findByLastnameOrderByFirstnameAsc", "Matthews");

        List<Person> actual = reactiveTemplate.find(query, Person.class).collectList().block();
        assertThat(actual)
                .hasSize(10)
                .containsExactlyElementsOf(allUsers);
    }

    @Test
    public void find_shouldWorkWithFilterEqualOrderByDesc() {
        createIndexIfNotExists(Person.class, "age_index", "age", IndexType.NUMERIC);
        createIndexIfNotExists(Person.class, "last_name_index", "lastname", IndexType.STRING);

        List<Person> allUsers = IntStream.rangeClosed(1, 10)
                .mapToObj(id -> new Person(nextId(), "Dave" + id, "Matthews")).collect(Collectors.toList());
        Collections.shuffle(allUsers); // Shuffle user list
        reactiveTemplate.insertAll(allUsers).blockLast();
        allUsers.sort((o1, o2) -> o2.getFirstname().compareTo(o1.getFirstname())); // Order user list by firstname descending

        Query query = createQueryForMethodWithArgs("findByLastnameOrderByFirstnameDesc", "Matthews");

        List<Person> actual = reactiveTemplate.find(query, Person.class).collectList().block();
        assertThat(actual)
                .hasSize(10)
                .containsExactlyElementsOf(allUsers);
    }

    @Test
    public void find_shouldWorkWithFilterRange() {
        createIndexIfNotExists(Person.class, "age_index", "age", IndexType.NUMERIC);

        List<Person> allUsers = IntStream.rangeClosed(21, 30)
                .mapToObj(age -> new Person(nextId(), "Dave" + age, "Matthews", age)).collect(Collectors.toList());
        reactiveTemplate.insertAll(allUsers).blockLast();

        Query query = createQueryForMethodWithArgs("findCustomerByAgeBetween", 25, 30);

        List<Person> actual = reactiveTemplate.find(query, Person.class).collectList().block();

        assertThat(actual)
                .hasSize(6)
                .containsExactlyInAnyOrderElementsOf(allUsers.subList(4, 10));
    }

}