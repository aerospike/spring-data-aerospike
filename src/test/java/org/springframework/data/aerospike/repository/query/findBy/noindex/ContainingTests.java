package org.springframework.data.aerospike.repository.query.findBy.noindex;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.Person;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeNullQueryCriterion.NULL_PARAM;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion.KEY;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion.KEY_VALUE_PAIR;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion.VALUE;

/**
 * Tests for the "Contains" repository query. Keywords: Containing, IsContaining, Contains.
 */
public class ContainingTests extends PersonRepositoryQueryTests {

    @Test
    void findBySimplePropertyContaining_String() {
        List<Person> persons = repository.findByFirstNameContaining("er");
        assertThat(persons).containsExactlyInAnyOrder(carter, oliver, leroi, leroi2);
    }

    @Test
    void findDistinctByStringSimplePropertyContaining() {
        List<Person> persons = repository.findDistinctByFirstNameContaining("er");
        assertThat(persons).hasSize(3);

        List<Person> persons2 = repository.findByFirstNameContaining("er");
        assertThat(persons2).hasSize(4);
    }

    @Test
    void findByNestedSimplePropertyContaining() {
        Address cartersAddress = carter.getAddress();
        Address davesAddress = dave.getAddress();

        carter.setAddress(new Address("Foo Street 2", 2, "C10124", "C0123"));
        repository.save(carter);
        dave.setAddress(new Address("Foo Street 1", 1, "C10123", "Bar"));
        repository.save(dave);
        boyd.setAddress(new Address(null, null, null, null));
        repository.save(boyd);

        List<Person> persons = repository.findByAddressZipCodeContaining("C10");
        assertThat(persons).containsExactlyInAnyOrder(carter, dave);

        carter.setAddress(cartersAddress);
        repository.save(carter);
        dave.setAddress(davesAddress);
        repository.save(dave);
    }

    @Test
    void findByArrayContaining() {
        assertThat(repository.findByIntArrayContaining(1)).containsOnly(matias);
        assertThat(repository.findByIntArrayContaining(5)).containsOnly(matias, leroi);
        assertThat(repository.findByIntArrayContaining(10)).containsOnly(leroi);
    }

    @Test
    void findByCollectionContaining() {
        assertThat(repository.findByStringsContaining("str1")).containsOnly(dave, donny);
        assertThat(repository.findByStringsContaining("str2")).containsOnly(dave, donny);
        assertThat(repository.findByStringsContaining("str3")).containsOnly(donny);
        assertThat(repository.findByStringsContaining("str5")).isEmpty();

        assertThat(repository.findByIntsContaining(550)).containsOnly(oliver, alicia);
        assertThat(repository.findByIntsContaining(990)).containsOnly(oliver, alicia);
        assertThat(repository.findByIntsContaining(600)).containsOnly(alicia);
        assertThat(repository.findByIntsContaining(7777)).isEmpty();
    }

    @Test
    void findByCollectionContainingNull() {
        List<String> strings = new ArrayList<>();
        strings.add(null);
        strings.add("null");
        stefan.setStrings(strings);
        repository.save(stefan);
        assertThat(repository.findByStringsContaining(NULL_PARAM)).contains(stefan);

        stefan.setStrings(null); // cleanup
        repository.save(stefan);
    }

    @Test
    void findByCollectionContaining_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByIntsContaining())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.ints CONTAINING: invalid number of arguments, expecting one");
    }

    @Test
    void findByCollectionContainingBoolean() {
        oliver.setListOfBoolean(List.of(true));
        repository.save(oliver);
        alicia.setListOfBoolean(List.of(true));
        repository.save(alicia);

        assertThat(repository.findByListOfBooleanContaining(true)).containsOnly(oliver, alicia);
    }

    @Test
    void findByCollectionContainingPOJO() {
        Address address1 = new Address("Foo Street 1", 1, "C0123", "Bar");
        Address address2 = new Address("Foo Street 2", 1, "C0123", "Bar");
        Address address3 = new Address("Foo Street 2", 1, "C0124", "Bar");
        Address address4 = new Address("Foo Street 1234", 1, "C01245", "Bar");

        List<Address> listOfAddresses1 = List.of(address1, address2, address3, address4);
        List<Address> listOfAddresses2 = List.of(address1, address2, address3);
        List<Address> listOfAddresses3 = List.of(address1, address2, address3);
        List<Address> listOfAddresses4 = List.of(address4);
        stefan.setAddressesList(listOfAddresses1);
        repository.save(stefan);
        douglas.setAddressesList(listOfAddresses2);
        repository.save(douglas);
        matias.setAddressesList(listOfAddresses3);
        repository.save(matias);
        leroi2.setAddressesList(listOfAddresses4);
        repository.save(leroi2);

        List<Person> persons;
        persons = repository.findByAddressesListContaining(address4);
        assertThat(persons).containsOnly(stefan, leroi2);

        persons = repository.findByAddressesListContaining(address1);
        assertThat(persons).containsOnly(stefan, douglas, matias);

        persons = repository.findByAddressesListContaining(new Address("Foo Street 12345", 12345, "12345", "Bar12345"));
        assertThat(persons).isEmpty();

        stefan.setAddressesList(null);
        repository.save(stefan);
        douglas.setAddressesList(null);
        repository.save(douglas);
        matias.setAddressesList(null);
        repository.save(matias);
        leroi2.setAddressesList(null);
        repository.save(leroi2);
    }

    @Test
    void findByMapKeysContainingString() {
        assertThat(donny.getStringMap()).containsKey("key1");
        assertThat(boyd.getStringMap()).containsKey("key1");

        List<Person> persons = repository.findByStringMapContaining(KEY, "key1");
        assertThat(persons).contains(donny, boyd);
    }

    @Test
    void findByMapValuesContainingString() {
        assertThat(donny.getStringMap()).containsValue("val1");
        assertThat(boyd.getStringMap()).containsValue("val1");

        List<Person> persons = repository.findByStringMapContaining(VALUE, "val1");
        assertThat(persons).contains(donny, boyd);
    }

    @Test
    void findByMapValuesContainingList() {
        Map<String, List<Integer>> mapOfLists1 = Map.of("0", List.of(100), "1", List.of(200));
        Map<String, List<Integer>> mapOfLists2 = Map.of("0", List.of(101), "1", List.of(201));
        Map<String, List<Integer>> mapOfLists3 = Map.of("1", List.of(201), "2", List.of(202));
        Map<String, List<Integer>> mapOfLists4 = Map.of("2", List.of(1000), "3", List.of(2000));
        stefan.setMapOfIntLists(mapOfLists1);
        repository.save(stefan);
        douglas.setMapOfIntLists(mapOfLists2);
        repository.save(douglas);
        matias.setMapOfIntLists(mapOfLists3);
        repository.save(matias);
        leroi2.setMapOfIntLists(mapOfLists4);
        repository.save(leroi2);

        List<Person> persons = repository.findByMapOfIntListsContaining(VALUE, List.of(100));
        assertThat(persons).contains(stefan);
        List<Person> persons2 = repository.findByMapOfIntListsContaining(VALUE, List.of(201));
        assertThat(persons2).contains(douglas, matias);
        List<Person> persons3 = repository.findByMapOfIntListsContaining(VALUE, List.of(20000));
        assertThat(persons3).isEmpty();
    }

    @Test
    void findByExactMapKeyAndValue_Boolean() {
        oliver.setMapOfBoolean(Map.of("test", true));
        repository.save(oliver);
        alicia.setMapOfBoolean(Map.of("test", true));
        repository.save(alicia);

        assertThat(repository.findByMapOfBooleanContaining(KEY_VALUE_PAIR, "test", true))
            .containsOnly(oliver, alicia);
    }

    @Test
    void findByExactMapKeyAndValue_List() {
        Map<String, List<Integer>> mapOfLists1 = Map.of("0", List.of(100), "1", List.of(200));
        Map<String, List<Integer>> mapOfLists2 = Map.of("0", List.of(100), "1", List.of(201));
        Map<String, List<Integer>> mapOfLists3 = Map.of("1", List.of(201), "2", List.of(202));
        Map<String, List<Integer>> mapOfLists4 = Map.of("2", List.of(202), "3", List.of(2000));
        stefan.setMapOfIntLists(mapOfLists1);
        repository.save(stefan);
        douglas.setMapOfIntLists(mapOfLists2);
        repository.save(douglas);
        matias.setMapOfIntLists(mapOfLists3);
        repository.save(matias);
        leroi2.setMapOfIntLists(mapOfLists4);
        repository.save(leroi2);

        List<Person> persons;
        persons = repository.findByMapOfIntListsContaining(KEY_VALUE_PAIR, "0", List.of(100));
        assertThat(persons).containsExactlyInAnyOrder(stefan, douglas);

        persons = repository.findByMapOfIntListsContaining(KEY_VALUE_PAIR, "2", List.of(202));
        assertThat(persons).containsExactlyInAnyOrder(matias, leroi2);

        persons = repository.findByMapOfIntListsContaining(KEY_VALUE_PAIR, "34", List.of(2000));
        assertThat(persons).isEmpty();
    }

    @Test
    void findByExactMapKeyAndValue_POJO() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            Address address1 = new Address("Foo Street 1", 1, "C0123", "Bar");
            Address address2 = new Address("Foo Street 2", 1, "C0123", "Bar");
            Address address3 = new Address("Foo Street 2", 1, "C0124", "Bar");
            Address address4 = new Address("Foo Street 1234", 1, "C01245", "Bar");

            Map<String, Address> mapOfAddresses1 = Map.of("a", address1);
            Map<String, Address> mapOfAddresses2 = Map.of("b", address2, "a", address1);
            Map<String, Address> mapOfAddresses3 = Map.of("c", address3, "a", address1);
            Map<String, Address> mapOfAddresses4 = Map.of("d", address4, "a", address1, "b", address2);
            stefan.setAddressesMap(mapOfAddresses1);
            repository.save(stefan);
            douglas.setAddressesMap(mapOfAddresses2);
            repository.save(douglas);
            matias.setAddressesMap(mapOfAddresses3);
            repository.save(matias);
            leroi2.setAddressesMap(mapOfAddresses4);
            repository.save(leroi2);

            List<Person> persons;
            persons = repository.findByAddressesMapContaining(KEY_VALUE_PAIR, "a", address1);
            assertThat(persons).containsExactlyInAnyOrder(stefan, douglas, matias, leroi2);

            persons = repository.findByAddressesMapContaining(KEY_VALUE_PAIR, "b", address2);
            assertThat(persons).containsExactlyInAnyOrder(douglas, leroi2);

            persons = repository.findByAddressesMapContaining(KEY_VALUE_PAIR, "cd", address3);
            assertThat(persons).isEmpty();

            stefan.setAddressesMap(null);
            repository.save(stefan);
            douglas.setAddressesMap(null);
            repository.save(douglas);
            matias.setAddressesMap(null);
            repository.save(matias);
            leroi2.setAddressesMap(null);
            repository.save(leroi2);
        }
    }

    @Test
    void findByExactMapKeyAndValue_Integer() {
        assertThat(carter.getIntMap()).containsKey("key1");
        assertThat(carter.getIntMap()).containsValue(0);

        List<Person> persons;
        persons = repository.findByIntMapContaining(KEY_VALUE_PAIR, "key1", 0);
        assertThat(persons).containsExactlyInAnyOrder(carter);
    }

    @Test
    void findByMapContainingNullValue() {
        Map<String, String> stringMap = new HashMap<>();
        stringMap.put("key", null);
        stefan.setStringMap(stringMap);
        repository.save(stefan);

        // find Persons with stringMap containing null value (regardless of a key)
        assertThat(repository.findByStringMapContaining(VALUE, NULL_PARAM)).contains(stefan);

        // Currently getting key-specific results for a Map requires 2 steps:
        // firstly query for all entities with existing map key
        List<Person> personsWithMapKeyExists = repository.findByStringMapContaining(KEY, "key");
        // and then leave only the records that have the key's value == null
        List<Person> personsWithMapValueNull = personsWithMapKeyExists.stream()
            .filter(person -> person.getStringMap().get("key") == null).toList();
        assertThat(personsWithMapValueNull).contains(stefan);

        stefan.setStringMap(null); // cleanup
        repository.save(stefan);
    }

    @Test
    void findByMapContaining_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByStringMapContaining())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.stringMap CONTAINING: invalid number of arguments, at least two arguments are " +
                "required");

        assertThatThrownBy(() -> negativeTestsRepository.findByStringMapContaining("key1"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.stringMap CONTAINING: invalid number of arguments, at least two arguments are " +
                "required");

        assertThatThrownBy(() -> negativeTestsRepository.findByStringMapContaining(1100))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.stringMap CONTAINING: invalid number of arguments, at least two arguments are " +
                "required");

        assertThatThrownBy(() -> negativeTestsRepository.findByStringMapContaining(KEY, VALUE))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.stringMap CONTAINING: invalid combination of arguments, cannot have multiple " +
                "AerospikeQueryCriterion arguments");

        assertThatThrownBy(() -> negativeTestsRepository.findByStringMapContaining("key", "value", new Person("id",
            "firstName"), "value"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.stringMap CONTAINING: invalid combination of arguments, the first one is required " +
                "to be AerospikeQueryCriterion");

        assertThatThrownBy(() -> negativeTestsRepository.findByIntMapContaining(KEY, 100))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.intMap CONTAINING: invalid map key type at position 2");

        assertThatThrownBy(() -> negativeTestsRepository.findByIntMapContaining(VALUE, "test"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.intMap CONTAINING: invalid map value type at position 2");

        assertThatThrownBy(() -> negativeTestsRepository.findByIntMapContaining(KEY_VALUE_PAIR, "test"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.intMap CONTAINING: invalid number of arguments, expecting three");

        assertThatThrownBy(() -> negativeTestsRepository.findByIntMapContaining(KEY_VALUE_PAIR, "test", "test2"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.intMap CONTAINING: invalid map value type at position 3");
    }
}
