package org.springframework.data.aerospike.core;

import org.junit.jupiter.api.Test;
import org.springframework.core.convert.ConverterNotFoundException;
import org.springframework.data.aerospike.core.model.GroupedEntities;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.mapping.MappingException;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.utility.AerospikeUniqueId.nextId;

public interface AbstractFindByEntitiesTest {

    @Test
    default void shouldFindAllRequestedEntities() {
        List<Person> persons = generatePersons(5);
        List<Customer> customers = generateCustomers(3);

        GroupedKeys groupedKeys = getGroupedKeys(persons, customers);
        GroupedEntities byIds = findByIds(groupedKeys);

        assertThat(byIds.getEntitiesByClass(Person.class)).containsExactlyInAnyOrderElementsOf(persons);
        assertThat(byIds.getEntitiesByClass(Customer.class)).containsExactlyInAnyOrderElementsOf(customers);
    }

    @Test
    default void shouldReturnAnEmptyResultIfKeysWhereSetToWrongEntities() {
        List<Person> persons = generatePersons(5);
        List<Customer> customers = generateCustomers(3);

        Set<String> requestedPersonsIds = persons.stream()
                .map(Person::getId)
                .collect(Collectors.toSet());
        Set<String> requestedCustomerIds = customers.stream().map(Customer::getId)
                .collect(Collectors.toSet());

        GroupedKeys groupedKeys = GroupedKeys.builder()
                .entityKeys(Person.class, requestedCustomerIds)
                .entityKeys(Customer.class, requestedPersonsIds)
                .build();

        GroupedEntities byIds = findByIds(groupedKeys);

        assertThat(byIds.containsEntities()).isFalse();
    }

    @Test
    default void shouldFindSomeOfIdsOfRequestedEntities() {
        List<Person> persons = generatePersons(2);
        List<Customer> customers = generateCustomers(3);

        GroupedKeys requestMapWithRandomExtraIds = getGroupedEntitiesKeysWithRandomExtraIds(persons, customers);
        GroupedEntities results = findByIds(requestMapWithRandomExtraIds);

        assertThat(results.getEntitiesByClass(Person.class)).containsExactlyInAnyOrderElementsOf(persons);
        assertThat(results.getEntitiesByClass(Customer.class)).containsExactlyInAnyOrderElementsOf(customers);
    }

    @Test
    default void shouldFindResultsOfOneOfRequestedEntity() {
        List<Person> persons = generatePersons(3);

        GroupedKeys groupedKeysWithRandomExtraIds = getGroupedEntitiesKeysWithRandomExtraIds(persons, emptyList());
        GroupedEntities results = findByIds(groupedKeysWithRandomExtraIds);

        assertThat(results.getEntitiesByClass(Person.class)).containsExactlyInAnyOrderElementsOf(persons);
        assertThat(results.getEntitiesByClass(Customer.class)).containsExactlyInAnyOrderElementsOf(emptyList());
    }

    @Test
    default void shouldFindForOneEntityIfAnotherContainsEmptyRequestList() {
        List<Person> persons = generatePersons(3);

        GroupedKeys groupedKeys = getGroupedKeys(persons, emptyList());
        GroupedEntities batchGroupedEntities = findByIds(groupedKeys);

        assertThat(batchGroupedEntities.getEntitiesByClass(Person.class)).containsExactlyInAnyOrderElementsOf(persons);
        assertThat(batchGroupedEntities.getEntitiesByClass(Customer.class)).containsExactlyInAnyOrderElementsOf(emptyList());
    }

    @Test
    default void shouldReturnMapWithEmptyResultsOnEmptyRequest() {
        GroupedKeys groupedKeys = GroupedKeys.builder()
                .entityKeys(Person.class, emptyList())
                .entityKeys(Customer.class, emptyList())
                .build();


        GroupedEntities batchGroupedEntities = findByIds(groupedKeys);

        assertThat(batchGroupedEntities.getEntitiesByClass(Person.class))
                .containsExactlyInAnyOrderElementsOf(emptyList());
        assertThat(batchGroupedEntities.getEntitiesByClass(Customer.class))
                .containsExactlyInAnyOrderElementsOf(emptyList());
    }

    @Test
    default void shouldReturnMapWithEmptyResultsIfNoEntitiesWhereFound() {
        GroupedKeys groupedKeys = GroupedKeys.builder()
                .entityKeys(Person.class, singletonList(nextId()))
                .entityKeys(Customer.class, singletonList(nextId()))
                .build();

        GroupedEntities batchGroupedEntities = findByIds(groupedKeys);

        assertThat(batchGroupedEntities.getEntitiesByClass(Person.class))
                .containsExactlyInAnyOrderElementsOf(emptyList());
        assertThat(batchGroupedEntities.getEntitiesByClass(Customer.class))
                .containsExactlyInAnyOrderElementsOf(emptyList());
    }

    @Test
    default void shouldThrowMappingExceptionOnNonAerospikeEntityClass() {
        List<Person> persons = generatePersons(2);
        Set<String> personIds = persons.stream()
                .map(Person::getId)
                .collect(Collectors.toSet());

        GroupedKeys groupedKeys = GroupedKeys.builder()
                .entityKeys(Person.class, personIds)
                .entityKeys(String.class, singletonList(1L))
                .build();

        assertThatThrownBy(() -> findByIds(groupedKeys))
                .isInstanceOf(MappingException.class)
                .hasMessage("Couldn't find PersistentEntity for type class java.lang.String!");
    }

    @Test
    default void shouldReturnAnEmptyResultOnEmptyRequestMap() {
        GroupedKeys groupedKeys = GroupedKeys.builder().build();
        GroupedEntities byIds = findByIds(groupedKeys);
        assertThat(byIds.getEntitiesByClass(Person.class)).isEmpty();
    }

    @Test
    default void shouldThrowConverterNotFoundExceptionOnClassWithoutConverter() {
        GroupedKeys groupedKeys = GroupedKeys.builder()
                .entityKeys(Person.class, singletonList(Person.builder().id("id").build()))
                .build();

        assertThatThrownBy(() -> findByIds(groupedKeys))
                .isInstanceOf(ConverterNotFoundException.class)
                .hasMessageContaining("No converter found capable of converting from type");
    }

    default GroupedKeys getGroupedKeys(Collection<Person> persons, Collection<Customer> customers) {
        Set<String> requestedPersonsIds = persons.stream()
                .map(Person::getId)
                .collect(Collectors.toSet());
        Set<String> requestedCustomerIds = customers.stream().map(Customer::getId)
                .collect(Collectors.toSet());

        return GroupedKeys.builder()
                .entityKeys(Person.class, requestedPersonsIds)
                .entityKeys(Customer.class, requestedCustomerIds)
                .build();
    }

    default GroupedKeys getGroupedEntitiesKeysWithRandomExtraIds(Collection<Person> persons, Collection<Customer> customers) {
        Set<String> requestedPersonsIds = Stream.concat(persons.stream().map(Person::getId), Stream.of(nextId(), nextId()))
                .collect(Collectors.toSet());
        Set<String> requestedCustomerIds = Stream.concat(customers.stream().map(Customer::getId), Stream.of(nextId(), nextId()))
                .collect(Collectors.toSet());

        return GroupedKeys.builder()
                .entityKeys(Person.class, requestedPersonsIds)
                .entityKeys(Customer.class, requestedCustomerIds)
                .build();
    }

    default List<Customer> generateCustomers(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> Customer.builder().id(nextId())
                        .firstname("firstName" + i)
                        .lastname("Smith")
                        .build())
                .peek(this::save)
                .collect(Collectors.toList());
    }

    default List<Person> generatePersons(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> Person.builder().id(nextId())
                        .firstName("firstName" + i)
                        .emailAddress("gmail.com")
                        .build())
                .peek(this::save)
                .collect(Collectors.toList());
    }

    <T> void save(T obj);
    GroupedEntities findByIds(GroupedKeys groupedKeys);
}
