package org.springframework.data.aerospike.core.reactive;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.util.QueryUtils;
import org.springframework.data.domain.Sort;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion.KEY;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion.KEY_VALUE_PAIR;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion.VALUE;
import static org.springframework.data.domain.Sort.Order.asc;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReactiveAerospikeTemplateFindByQueryTests extends BaseReactiveIntegrationTests {

    @BeforeAll
    public void beforeAllSetUp() {
        additionalAerospikeTestOperations.deleteAllAndVerify(Person.class);
        additionalAerospikeTestOperations.deleteAllAndVerify(Person.class, OVERRIDE_SET_NAME);
    }

    @Override
    @BeforeEach
    public void setUp() {
        additionalAerospikeTestOperations.deleteAllAndVerify(Person.class);
        additionalAerospikeTestOperations.deleteAllAndVerify(Person.class, OVERRIDE_SET_NAME);
        super.setUp();
    }

    @Test
    public void findAll_OrderByFirstName() {
        List<Person> persons = new ArrayList<>();
        persons.add(new Person(nextId(), "Dave", "Matthews"));
        persons.add(new Person(nextId(), "Josh", "Matthews"));
        persons.add(new Person(nextId(), "Chris", "Yes"));
        persons.add(new Person(nextId(), "Kate", "New"));
        persons.add(new Person(nextId(), "Nicole", "Joshua"));
        reactiveTemplate.insertAll(persons).blockLast();

        Sort sort = Sort.by(asc("firstName"));
        List<Person> result = reactiveTemplate.findAll(sort, 0, 0, Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();

        assertThat(Objects.requireNonNull(result).stream().map(Person::getFirstName).collect(Collectors.toList()))
            .hasSize(5)
            .containsExactly("Chris", "Dave", "Josh", "Kate", "Nicole");

        deleteAll(persons); // cleanup
    }

    @Test
    public void find_throwsExceptionForUnsortedQueryWithSpecifiedOffsetValue() {
        Query query = new Query((Qualifier) null);
        query.setOffset(1);

        assertThatThrownBy(() -> reactiveTemplate.find(query, Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unsorted query must not have offset value. For retrieving paged results use sorted query.");
    }

    @Test
    public void findByFilterEqual() {
        List<Person> allUsers = IntStream.rangeClosed(1, 10)
            .mapToObj(id -> new Person(nextId(), "Dave", "Matthews")).collect(Collectors.toList());
        reactiveTemplate.insertAll(allUsers).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport, "findByFirstName", "Dave");

        List<Person> actual = reactiveTemplate.find(query, Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();
        assertThat(actual)
            .hasSize(10)
            .containsExactlyInAnyOrderElementsOf(allUsers);

        deleteAll(allUsers); // cleanup
    }

    @Test
    public void findByFilterEqualOrderBy() {
        List<Person> allUsers = IntStream.rangeClosed(1, 10)
            .mapToObj(id -> new Person(nextId(), "Dave" + id, "Matthews")).collect(Collectors.toList());
        Collections.shuffle(allUsers); // Shuffle user list
        reactiveTemplate.insertAll(allUsers).blockLast();
        allUsers.sort(Comparator.comparing(Person::getFirstName)); // Order user list by firstname ascending

        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport,
            "findByLastNameOrderByFirstNameAsc", "Matthews");

        List<Person> actual = reactiveTemplate.find(query, Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();
        assertThat(actual)
            .hasSize(10)
            .containsExactlyElementsOf(allUsers);

        deleteAll(allUsers); // cleanup
    }

    @Test
    public void findByFilterEqualOrderByDesc() {
        List<Person> allUsers = IntStream.rangeClosed(1, 10)
            .mapToObj(id -> new Person(nextId(), "Dave" + id, "Matthews")).collect(Collectors.toList());
        Collections.shuffle(allUsers); // Shuffle user list
        reactiveTemplate.insertAll(allUsers).blockLast();
        allUsers.sort((o1, o2) -> o2.getFirstName()
            .compareTo(o1.getFirstName())); // Order user list by firstname descending

        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport,
            "findByLastNameOrderByFirstNameDesc", "Matthews");

        List<Person> actual = reactiveTemplate.find(query, Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();
        assertThat(actual)
            .hasSize(10)
            .containsExactlyElementsOf(allUsers);

        deleteAll(allUsers); // cleanup
    }

    @Test
    public void findByFilterEqualOrderByDescWithSetName() {
        List<Person> allUsers = IntStream.rangeClosed(1, 10)
            .mapToObj(id -> new Person(nextId(), "Dave" + id, "Matthews")).collect(Collectors.toList());
        Collections.shuffle(allUsers); // Shuffle user list
        reactiveTemplate.insertAll(allUsers, OVERRIDE_SET_NAME).blockLast();
        allUsers.sort((o1, o2) -> o2.getFirstName()
            .compareTo(o1.getFirstName())); // Order user list by firstname descending

        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport,
            "findByLastNameOrderByFirstNameDesc", "Matthews");

        List<Person> actual = reactiveTemplate.find(query, Person.class, OVERRIDE_SET_NAME)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();
        assertThat(actual)
            .hasSize(10)
            .containsExactlyElementsOf(allUsers);

        deleteAll(allUsers, OVERRIDE_SET_NAME); // cleanup
    }

    @Test
    public void findByFilterRange() {
        List<Person> allUsers = IntStream.rangeClosed(21, 30)
            .mapToObj(age -> Person.builder().id(nextId()).firstName("Dave" + age).lastName("Matthews").age(age)
                .build())
            .collect(Collectors.toList());
        reactiveTemplate.insertAll(allUsers).blockLast();

        // upper limit is exclusive
        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport, "findCustomerByAgeBetween", 25, 31);

        List<Person> actual = reactiveTemplate.find(query, Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();

        assertThat(actual)
            .hasSize(6)
            .containsExactlyInAnyOrderElementsOf(allUsers.subList(4, 10));

        deleteAll(allUsers); // cleanup
    }

    @Test
    public void findByFilterRangeNonExisting() {
        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport, "findCustomerByAgeBetween", 100,
            150);

        List<Person> actual = reactiveTemplate.find(query, Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();

        assertThat(actual).isEmpty();
    }

    @Test
    public void findWithFilterEqualOrderByDescNonExisting() {
        Object[] args = {"NonExistingSurname"};
        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport,
            "findByLastNameOrderByFirstNameDesc", args);

        List<Person> result = reactiveTemplate.find(query, Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();

        assertThat(result).isEmpty();
    }

    @Test
    public void findByListContainingInteger() {
        List<Person> persons = IntStream.rangeClosed(1, 7)
            .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews")
                .ints(Collections.singletonList(100 * id)).build())
            .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport, "findByIntsContaining", 100);

        List<Person> result = reactiveTemplate.find(query, Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();
        assertThat(result)
            .hasSize(1)
            .containsExactlyInAnyOrderElementsOf(persons.subList(0, 1));

        deleteAll(persons); // cleanup
    }

    @Test
    public void findByListContainingString() {
        List<Person> persons = IntStream.rangeClosed(1, 7)
            .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews")
                .strings(Collections.singletonList("str" + id)).build())
            .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport, "findByStringsContaining", "str2");

        List<Person> result = reactiveTemplate.find(query, Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();
        assertThat(result)
            .hasSize(1)
            .containsExactlyInAnyOrderElementsOf(persons.subList(1, 2));

        deleteAll(persons); // cleanup
    }

    @Test
    public void findByMapKeysContaining() {
        List<Person> persons = IntStream.rangeClosed(1, 2)
            .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews")
                .stringMap(Collections.singletonMap("key" + id, "val" + id)).build())
            .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport, "findByStringMapContaining", KEY,
            "key1");

        List<Person> result = reactiveTemplate.find(query, Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();
        assertThat(result)
            .hasSize(1)
            .containsExactlyInAnyOrderElementsOf(persons.subList(0, 1));

        deleteAll(persons); // cleanup
    }

    @Test
    public void findByMapValuesContaining() {
        List<Person> persons = IntStream.rangeClosed(1, 2)
            .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews")
                .stringMap(Collections.singletonMap("key" + id, "val" + id)).build())
            .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport, "findByStringMapContaining",
            VALUE, "val1");

        List<Person> result = reactiveTemplate.find(query, Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();
        assertThat(result)
            .hasSize(1)
            .containsExactlyInAnyOrderElementsOf(persons.subList(0, 1));

        deleteAll(persons); // cleanup
    }

    @Test
    public void findByMapKeyValueContaining() {
        List<Person> persons = IntStream.rangeClosed(1, 5)
            .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews")
                .stringMap(Collections.singletonMap("key" + id, "val" + id)).build())
            .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport, "findByStringMapContaining",
            KEY_VALUE_PAIR, "key1",
            "val1");

        List<Person> result = reactiveTemplate.find(query, Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();
        assertThat(result)
            .hasSize(1)
            .containsExactlyInAnyOrderElementsOf(persons.subList(0, 1));

        deleteAll(persons); // cleanup
    }

    @Test
    public void findByMapKeyValue() {
        List<Person> persons = IntStream.rangeClosed(1, 5)
            .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews")
                .stringMap(Collections.singletonMap("key" + id, "val" + id)).build())
            .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport, "findByStringMapContaining",
            KEY_VALUE_PAIR, "key3",
            "val3");

        List<Person> result = reactiveTemplate.find(query, Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();
        assertThat(result)
            .hasSize(1)
            .containsExactlyInAnyOrderElementsOf(persons.subList(2, 3));

        deleteAll(persons); // cleanup
    }

    @Test
    public void findPersonsByFriendAge() {
        List<Person> persons = IntStream.rangeClosed(1, 5)
            .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews")
                .friend(new Person("person" + id, "Leroi" + id, 10 * id)).build())
            .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport, "findByFriendAge", 50);

        List<Person> result = reactiveTemplate.find(query, Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();
        assertThat(result)
            .hasSize(1)
            .containsExactlyInAnyOrderElementsOf(persons.subList(4, 5));

        deleteAll(persons); // cleanup
    }

    @Test
    public void findPersonsByFriendAgeNotEqual() {
        List<Person> persons = IntStream.rangeClosed(1, 5)
            .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews")
                .friend(new Person("person" + id, "Leroi" + id, 10 * id)).build())
            .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport, "findByFriendAgeIsNot", 50);

        List<Person> result = reactiveTemplate.find(query, Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();
        assertThat(result)
            .hasSize(4)
            .containsExactlyInAnyOrderElementsOf(persons.subList(0, 4));

        deleteAll(persons); // cleanup
    }

    @Test
    public void findPersonsByFriendAgeGreaterThan() {
        List<Person> persons = IntStream.rangeClosed(1, 5)
            .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews")
                .friend(new Person("person" + id, "Leroi" + id, 10 * id)).build())
            .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport, "findByFriendAgeGreaterThan", 42);

        List<Person> result = reactiveTemplate.find(query, Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();
        assertThat(result)
            .hasSize(1)
            .containsExactlyInAnyOrderElementsOf(persons.subList(4, 5));

        deleteAll(persons); // cleanup
    }

    @Test
    public void findPersonsByFriendAgeLessThanOrEqual() {
        List<Person> persons = IntStream.rangeClosed(1, 5)
            .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews")
                .friend(new Person("person" + id, "Leroi" + id, 10 * id)).build())
            .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport, "findByFriendAgeLessThanEqual", 42);

        List<Person> result = reactiveTemplate.find(query, Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();
        assertThat(result)
            .hasSize(4)
            .containsExactlyInAnyOrderElementsOf(persons.subList(0, 4));

        deleteAll(persons); // cleanup
    }

    @Test
    public void findPersonsByFriendAgeRange() {
        List<Person> persons = IntStream.rangeClosed(1, 5)
            .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews")
                .friend(new Person("person" + id, "Leroi" + id, 10 * id)).build())
            .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport, "findByFriendAgeBetween", 42, 51);

        List<Person> result = reactiveTemplate.find(query, Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();
        assertThat(result)
            .hasSize(1)
            .containsExactlyInAnyOrderElementsOf(persons.subList(4, 5));

        deleteAll(persons); // cleanup
    }

    @Test
    public void findPersonsByAddressZipCode() {
        List<Person> persons = IntStream.rangeClosed(1, 5)
            .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews")
                .address(new Address("Foo Street " + id, id, "C0123" + id, "City" + id)).build())
            .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport, "findByAddressZipCode", "C01233");

        List<Person> result = reactiveTemplate.find(query, Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();
        assertThat(result)
            .hasSize(1)
            .containsExactlyInAnyOrderElementsOf(persons.subList(2, 3));

        deleteAll(persons); // cleanup
    }

    @Test
    public void findByAddressZipCodeContaining() {
        List<Person> persons = IntStream.rangeClosed(1, 5)
            .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews")
                .address(new Address("Foo Street " + id, id, "C012" + id, "City" + id)).build())
            .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport, "findByAddressZipCodeContaining",
            "123");

        List<Person> result = reactiveTemplate.find(query, Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();
        assertThat(result)
            .hasSize(1)
            .containsExactlyInAnyOrderElementsOf(persons.subList(2, 3));

        deleteAll(persons); // cleanup
    }
}
