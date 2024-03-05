package org.springframework.data.aerospike.repository.query.findBy.noindex;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.Person;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeNullQueryCriteria.NULL_PARAM;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriteria.KEY;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriteria.VALUE;

/**
 * Tests for the "Does not contain" repository query. Keywords: IsNotContaining, NotContaining, NotContains.
 */
public class NotContainingTests extends PersonRepositoryQueryTests {

    @Test
    void findBySimplePropertyNotContaining_String() {
        List<Person> persons = repository.findByFirstNameNotContaining("er");
        assertThat(persons).containsExactlyInAnyOrder(dave, donny, alicia, boyd, stefan, matias, douglas);
    }

    @Test
    void findByNestedSimplePropertyNotContaining() {
        Address cartersAddress = carter.getAddress();
        Address davesAddress = dave.getAddress();
        Address boydsAddress = boyd.getAddress();

        carter.setAddress(new Address("Foo Street 2", 2, "C10124", "C0123"));
        repository.save(carter);
        dave.setAddress(new Address("Foo Street 1", 1, "C10123", "Bar"));
        repository.save(dave);
        boyd.setAddress(new Address("Foo Street 3", 3, "C112344123", "Bar"));
        repository.save(boyd);

        List<Person> persons = repository.findByAddressZipCodeNotContaining("C10");
        assertThat(persons).containsExactlyInAnyOrder(donny, boyd, oliver, alicia, stefan, leroi, leroi2, matias,
            douglas);

        carter.setAddress(cartersAddress);
        repository.save(carter);
        dave.setAddress(davesAddress);
        repository.save(dave);
        boyd.setAddress(boydsAddress);
        repository.save(boyd);
    }

    @Test
    void findByCollectionNotContainingString() {
        List<Person> persons = repository.findByStringsNotContaining("str5");
        assertThat(persons).containsExactlyInAnyOrderElementsOf(allPersons);
    }

    @Test
    void findByCollectionNotContainingPOJO() {
        Address address1 = new Address("Foo Street 1", 1, "C0123", "Bar");

        List<Address> listOfAddresses = List.of(address1);
        stefan.setAddressesList(listOfAddresses);
        repository.save(stefan);

        List<Person> persons;
        persons = repository.findByAddressesListNotContaining(address1);
        assertThat(persons).containsExactlyInAnyOrderElementsOf(
            allPersons.stream().filter(person -> !person.getFirstName().equals("Stefan")).collect(Collectors.toList()));

        stefan.setAddressesList(null);
        repository.save(stefan);
    }

    @Test
    void findByCollectionContainingNull() {
        List<String> strings = new ArrayList<>();
        strings.add("ing");
        stefan.setStrings(strings);
        repository.save(stefan);
        assertThat(repository.findByStringsNotContaining(NULL_PARAM)).contains(stefan);

        strings.add(null);
        stefan.setStrings(strings);
        repository.save(stefan);
        assertThat(repository.findByStringsNotContaining(NULL_PARAM)).doesNotContain(stefan);

        stefan.setStrings(null); // cleanup
        repository.save(stefan);
    }

    @Test
    void findByCollection_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByIntsContaining())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.ints CONTAINING: invalid number of arguments, expecting at least one");

        assertThatThrownBy(() -> negativeTestsRepository.findByIntsNotContaining())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.ints NOT_CONTAINING: invalid number of arguments, expecting at least one");
    }

    @Test
    void findByMapNotContainingKey_String() {
        assertThat(donny.getStringMap()).containsKey("key1");
        assertThat(boyd.getStringMap()).containsKey("key1");

        List<Person> persons = repository.findByStringMapNotContaining("key1", KEY);
        assertThat(persons).contains(dave, oliver, alicia, carter, stefan, leroi, leroi2, matias, douglas);
    }

    @Test
    void findByMapNotContainingValue_String() {
        assertThat(donny.getStringMap()).containsValue("val1");
        assertThat(boyd.getStringMap()).containsValue("val1");

        List<Person> persons = repository.findByStringMapNotContaining("val1", VALUE);
        assertThat(persons).contains(dave, oliver, alicia, carter, stefan, leroi, leroi2, matias, douglas);
    }

    @Test
    void findByMapNotContainingNullValue() {
        Map<String, String> stringMap = new HashMap<>();
        stringMap.put("key", "str");
        stefan.setStringMap(stringMap);
        repository.save(stefan);

        // find Persons with stringMap not containing null value (regardless of a key)
        assertThat(repository.findByStringMapNotContaining(NULL_PARAM, VALUE)).contains(stefan);

        // Currently getting key-specific results for a Map requires 2 steps:
        // firstly query for all entities with existing map key
        List<Person> personsWithMapKeyExists = repository.findByStringMapContaining("key", KEY);
        // and then leave only the records that have the key's value != null
        List<Person> personsWithMapValueNotNull = personsWithMapKeyExists.stream()
            .filter(person -> person.getStringMap().get("key") != null).toList();
        assertThat(personsWithMapValueNotNull).contains(stefan);

        stringMap.put("key", null);
        stefan.setStringMap(stringMap);
        repository.save(stefan);
        assertThat(repository.findByStringMapNotContaining(NULL_PARAM, VALUE)).doesNotContain(stefan);

        personsWithMapKeyExists = repository.findByStringMapContaining("key", KEY);
        personsWithMapValueNotNull = personsWithMapKeyExists.stream()
            .filter(person -> person.getStringMap().get("key") != null).toList();
        assertThat(personsWithMapValueNotNull).doesNotContain(stefan);

        stefan.setStringMap(null); // cleanup
        repository.save(stefan);
    }

    @Test
    void findByMapNotContaining_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByStringMapNotContaining())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.stringMap NOT_CONTAINING: invalid number of arguments, at least two arguments are " +
                "required");

        assertThatThrownBy(() -> negativeTestsRepository.findByStringMapNotContaining("key1"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.stringMap NOT_CONTAINING: invalid number of arguments, at least two arguments are " +
                "required");

        assertThatThrownBy(() -> negativeTestsRepository.findByStringMapNotContaining(1100))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.stringMap NOT_CONTAINING: invalid number of arguments, at least two arguments are " +
                "required");

        assertThatThrownBy(() -> negativeTestsRepository.findByStringMapNotContaining(KEY, VALUE))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.stringMap NOT_CONTAINING: invalid combination of arguments, cannot have multiple " +
                "MapCriteria arguments");

        assertThatThrownBy(() -> negativeTestsRepository.findByStringMapNotContaining("key", "value", new Person("id"
            , "firstName"), "value"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.stringMap NOT_CONTAINING: invalid argument type, expected String, Number or byte[] at" +
                " position 3");
    }
}
