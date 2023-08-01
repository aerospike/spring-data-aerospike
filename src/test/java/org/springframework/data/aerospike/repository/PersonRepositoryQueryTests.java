package org.springframework.data.aerospike.repository;

import com.aerospike.client.Value;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.IndexUtils;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.PersonRepository;
import org.springframework.data.aerospike.sample.PersonSomeFields;
import org.springframework.data.aerospike.utility.TestUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.AsCollections.of;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeMapCriteria.KEY;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeMapCriteria.VALUE;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeMapCriteria.VALUE_CONTAINING;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PersonRepositoryQueryTests extends BaseBlockingIntegrationTests {

    static final Person dave = Person.builder().id(nextId()).firstName("Dave").lastName("Matthews").age(42)
        .strings(List.of("str0", "str1", "str2")).address(new Address("Foo Street 1", 1, "C0123", "Bar"))
        .build();
    static final Person donny = Person.builder().id(nextId()).firstName("Donny").lastName("Macintire").age(39)
        .strings(List.of("str1", "str2", "str3")).stringMap(of("key1", "val1")).build();
    static final Person oliver = Person.builder().id(nextId()).firstName("Oliver August").lastName("Matthews").age(14)
        .ints(List.of(425, 550, 990)).build();
    static final Person alicia = Person.builder().id(nextId()).firstName("Alicia").lastName("Keys").age(30)
        .ints(List.of(550, 600, 990)).build();
    static final Person carter = Person.builder().id(nextId()).firstName("Carter").lastName("Beauford").age(49)
        .intMap(of("key1", 0, "key2", 1))
        .address(new Address("Foo Street 2", 2, "C0124", "C0123")).build();
    static final Person boyd = Person.builder().id(nextId()).firstName("Boyd").lastName("Tinsley").age(45)
        .stringMap(of("key1", "val1", "key2", "val2")).address(new Address(null, null, null, null))
        .build();
    static final Person stefan = Person.builder().id(nextId()).firstName("Stefan").lastName("Lessard").age(34).build();
    static final Person leroi = Person.builder().id(nextId()).firstName("Leroi").lastName("Moore").age(44).build();
    static final Person leroi2 = Person.builder().id(nextId()).firstName("Leroi").lastName("Moore").age(25).build();
    static final Person matias = Person.builder().id(nextId()).firstName("Matias").lastName("Craft").age(24).build();
    static final Person douglas = Person.builder().id(nextId()).firstName("Douglas").lastName("Ford").age(25).build();
    public static final List<Person> allPersons = List.of(dave, donny, oliver, alicia, carter, boyd, stefan,
        leroi, leroi2, matias, douglas);
    @Autowired
    PersonRepository<Person> repository;

    @BeforeAll
    public void beforeAll() {
        indexRefresher.refreshIndexes();
        repository.deleteAll(allPersons);
        repository.saveAll(allPersons);
    }

    @AfterAll
    public void afterAll() {
        repository.deleteAll(allPersons);
    }

    @Test
    void findByListContainingString_forExistingResult() {
        assertThat(repository.findByStringsContaining("str1")).containsOnly(dave, donny);
        assertThat(repository.findByStringsContaining("str2")).containsOnly(dave, donny);
        assertThat(repository.findByStringsContaining("str3")).containsOnly(donny);
    }

    @Test
    void findByListContainingString_forEmptyResult() {
        List<Person> persons = repository.findByStringsContaining("str5");
        assertThat(persons).isEmpty();
    }

    @Test
    void findByListNotContainingString() {
        List<Person> persons = repository.findByStringsNotContaining("str5");
        assertThat(persons).containsExactlyInAnyOrderElementsOf(allPersons);
    }

    @Test
    void findByListContainingInteger_forExistingResult() {
        assertThat(repository.findByIntsContaining(550)).containsOnly(oliver, alicia);
        assertThat(repository.findByIntsContaining(990)).containsOnly(oliver, alicia);
        assertThat(repository.findByIntsContaining(600)).containsOnly(alicia);

        assertThat(repository.findByIntsContaining(550, 990)).containsOnly(oliver, alicia);
        assertThat(repository.findByIntsContaining(550, 990, 600)).containsOnly(alicia);
    }

    @Test
    void findByListContainingInteger_forEmptyResult() {
        List<Person> persons = repository.findByIntsContaining(7777);
        assertThat(persons).isEmpty();
    }

    @Test
    void findByListContainingBoolean() {
        oliver.setListOfBoolean(List.of(true));
        repository.save(oliver);
        alicia.setListOfBoolean(List.of(true));
        repository.save(alicia);

        assertThat(repository.findByListOfBooleanContaining(true)).containsOnly(oliver, alicia);
    }

    @Test
    void findByListContainingAddress() {
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
    void findByListNotContainingAddress() {
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
    void findByBooleanInt() {
        boolean initialValue = Value.UseBoolBin;
        Value.UseBoolBin = false; // save boolean as int
        Person intBoolBinPerson = Person.builder().id(nextId()).isActive(true).firstName("Test").build();
        repository.save(intBoolBinPerson);

        List<Person> persons;
        persons = repository.findByIsActive(true);
        assertThat(persons).contains(intBoolBinPerson);

        persons = repository.findByIsActiveTrue(); // another way to call the query method
        assertThat(persons).contains(intBoolBinPerson);

        persons = repository.findByIsActiveFalse();
        assertThat(persons).doesNotContain(intBoolBinPerson);
        assertThat(persons).contains(leroi);

        Value.UseBoolBin = initialValue; // set back to the default value
        repository.delete(intBoolBinPerson);
    }

    @Test
    void findByBoolean() {
        boolean initialValue = Value.UseBoolBin;
        Value.UseBoolBin = true; // save boolean as bool
        Person intBoolBinPerson = Person.builder().id(nextId()).isActive(true).firstName("Test").build();
        repository.save(intBoolBinPerson);

        List<Person> persons = repository.findByIsActive(true);
        assertThat(persons).contains(intBoolBinPerson);

        Value.UseBoolBin = initialValue; // set back to the default value
        repository.delete(intBoolBinPerson);
    }

    @Test
    void findByListValueLessThanOrEqualNumber() {
        List<Person> persons;
        persons = repository.findByIntsLessThanEqual(500);
        assertThat(persons).containsOnly(oliver);

        persons = repository.findByIntsLessThanEqual(Long.MAX_VALUE - 1);
        assertThat(persons).containsOnly(oliver, alicia);

        persons = repository.findByIntsLessThanEqual(Long.MAX_VALUE);
        assertThat(persons).containsOnly(oliver, alicia);
    }

    @Test
    void findByListValueLessThanOrEqualString() {
        List<Person> persons;
        persons = repository.findByStringsLessThanEqual("str4");
        assertThat(persons).containsOnly(dave, donny);

        persons = repository.findByStringsLessThanEqual("str3");
        assertThat(persons).containsOnly(dave, donny);

        persons = repository.findByStringsLessThanEqual("str2");
        assertThat(persons).containsOnly(dave, donny);

        persons = repository.findByStringsLessThanEqual("str0");
        assertThat(persons).containsOnly(dave);

        persons = repository.findByStringsLessThanEqual("str");
        assertThat(persons).isEmpty();
    }

    @Test
    void findByListValueGreaterThanNumber() {
        List<Person> persons;
        persons = repository.findByIntsGreaterThan(549);
        assertThat(persons).containsOnly(oliver, alicia);

        persons = repository.findByIntsGreaterThan(990);
        assertThat(persons).isEmpty();

        persons = repository.findByIntsGreaterThan(Long.MIN_VALUE);
        assertThat(persons).containsOnly(oliver, alicia);

        persons = repository.findByIntsGreaterThan(Long.MAX_VALUE - 1);
        assertThat(persons).isEmpty();

        assertThatThrownBy(() -> repository.findByIntsGreaterThan(Long.MAX_VALUE))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("LIST_VAL_GT FilterExpression unsupported value: expected [Long.MIN_VALUE..Long.MAX_VALUE-1]");
    }

    @Test
    void findByListValueGreaterThanString() {
        List<Person> persons;
        persons = repository.findByStringsGreaterThan("str0");
        assertThat(persons).containsOnly(dave, donny);

        persons = repository.findByStringsGreaterThan("");
        assertThat(persons).containsOnly(dave, donny);

        // ordering is by each byte in a String, so "t" > "str" because "t" > "s"
        persons = repository.findByStringsGreaterThan("t");
        assertThat(persons).isEmpty();
    }

    @Test
//    Note: only the upper level ListOfLists will be compared even if the parameter has different number of levels
//    So findByListOfListsGreaterThan(List.of(1)) and findByListOfListsGreaterThan(List.of(List.of(List.of(1))))
//    will compare with the given parameter only the upper level ListOfLists itself
    void findByListOfListsGreaterThan() {
        List<List<Integer>> listOfLists1 = List.of(List.of(100));
        List<List<Integer>> listOfLists2 = List.of(List.of(101));
        List<List<Integer>> listOfLists3 = List.of(List.of(102));
        List<List<Integer>> listOfLists4 = List.of(List.of(1000));
        stefan.setListOfIntLists(listOfLists1);
        repository.save(stefan);
        douglas.setListOfIntLists(listOfLists2);
        repository.save(douglas);
        matias.setListOfIntLists(listOfLists3);
        repository.save(matias);
        leroi2.setListOfIntLists(listOfLists4);
        repository.save(leroi2);

        List<Person> persons;
        persons = repository.findByListOfIntListsGreaterThan(List.of(List.of(99)));
        assertThat(persons).containsOnly(stefan, douglas, matias, leroi2);

        persons = repository.findByListOfIntListsGreaterThan(List.of(List.of(100)));
        assertThat(persons).containsOnly(douglas, matias, leroi2);

        persons = repository.findByListOfIntListsGreaterThan(List.of(List.of(102)));
        assertThat(persons).containsOnly(leroi2);

        persons = repository.findByListOfIntListsGreaterThan(List.of(List.of(401)));
        assertThat(persons).containsOnly(leroi2);

        persons = repository.findByListOfIntListsGreaterThan(List.of(List.of(4000)));
        assertThat(persons).isEmpty();
    }

    @Test
    void findByListGreaterThan() {
        List<Integer> listToCompare1 = List.of(100, 200, 300, 400);
        List<Integer> listToCompare2 = List.of(425, 550);
        List<Integer> listToCompare3 = List.of(426, 551, 991);
        List<Integer> listToCompare4 = List.of(1000, 2000, 3000, 4000);
        List<Integer> listToCompare5 = List.of(551, 601, 991);
        List<Integer> listToCompare6 = List.of(550, 600, 990);

        List<Person> persons;
        persons = repository.findByIntsGreaterThan(listToCompare1);
        assertThat(persons).containsOnly(oliver, alicia);

        persons = repository.findByIntsGreaterThan(listToCompare2);
        assertThat(persons).containsOnly(oliver, alicia);

        persons = repository.findByIntsGreaterThan(listToCompare3);
        assertThat(persons).containsOnly(alicia);

        persons = repository.findByIntsGreaterThan(listToCompare4);
        assertThat(persons).isEmpty();

        persons = repository.findByIntsGreaterThan(listToCompare5);
        assertThat(persons).isEmpty();

        persons = repository.findByIntsGreaterThan(listToCompare6);
        assertThat(persons).isEmpty();
    }

    @Test
    void findByIntegerListValueInRange() {
        List<Person> persons = repository.findByIntsBetween(500, 600);
        assertThat(persons).containsExactlyInAnyOrder(oliver, alicia);
    }

    @Test
    void findByStringListValueInRange() {
        List<Person> persons;
        persons = repository.findByStringsBetween("str1", "str3");
        assertThat(persons).containsExactlyInAnyOrder(donny, dave);

        persons = repository.findByStringsBetween("str3", "str3"); // upper limit is exclusive
        assertThat(persons).isEmpty();

        persons = repository.findByStringsBetween("str3", "str4");
        assertThat(persons).containsExactlyInAnyOrder(donny);
    }

    @Test
    void findByIntegerListInRange() {
        List<Integer> list1 = List.of(100, 200, 300);
        List<Integer> list2 = List.of(1000, 2000, 3000);

        List<Person> persons = repository.findByIntsBetween(list1, list2);
        assertThat(persons).containsExactlyInAnyOrder(oliver, alicia);
    }

    @Test
    void findByStringListInRange() {
        List<String> list1 = List.of("str", "str1");
        List<String> list2 = List.of("str55", "str65");

        List<Person> persons = repository.findByStringsBetween(list1, list2);
        assertThat(persons).containsExactlyInAnyOrder(dave, donny);
    }

    @Test
    void findByMapKeysContainingString() {
        assertThat(donny.getStringMap()).containsKey("key1");
        assertThat(boyd.getStringMap()).containsKey("key1");

        List<Person> persons = repository.findByStringMapContaining("key1", KEY);
        assertThat(persons).contains(donny, boyd);

        List<Person> persons2 = repository.findByStringMapContaining("key1", "key2", KEY);
        assertThat(persons2).contains(boyd);

        List<Person> persons3 = repository.findByStringMapContaining("key1", "key2", "key3", KEY);
        assertThat(persons3).isEmpty();
    }

    @Test
    void findByMapKeysNotContainingString() {
        assertThat(donny.getStringMap()).containsKey("key1");
        assertThat(boyd.getStringMap()).containsKey("key1");

        List<Person> persons = repository.findByStringMapNotContaining("key1", KEY);
        assertThat(persons).contains(dave, oliver, alicia, carter, stefan, leroi, leroi2, matias, douglas);
    }

    @Test
    void findByMapValuesContainingString() {
        assertThat(donny.getStringMap()).containsValue("val1");
        assertThat(boyd.getStringMap()).containsValue("val1");

        List<Person> persons = repository.findByStringMapContaining("val1", VALUE);
        assertThat(persons).contains(donny, boyd);
        List<Person> persons2 = repository.findByStringMapContaining("val1", "val2", VALUE);
        assertThat(persons2).contains(boyd);
        List<Person> persons3 = repository.findByStringMapContaining("val1", "val2", "val3", VALUE);
        assertThat(persons3).isEmpty();
    }

    @Test
    void findByMapValuesNotContainingString() {
        assertThat(donny.getStringMap()).containsValue("val1");
        assertThat(boyd.getStringMap()).containsValue("val1");

        List<Person> persons = repository.findByStringMapNotContaining("val1", VALUE);
        assertThat(persons).contains(dave, oliver, alicia, carter, stefan, leroi, leroi2, matias, douglas);
    }

    @Test
    void findByMapContainingBoolean() {
        oliver.setMapOfBoolean(Map.of("test", true));
        repository.save(oliver);
        alicia.setMapOfBoolean(Map.of("test", true));
        repository.save(alicia);

        assertThat(repository.findByMapOfBooleanContaining("test", true)).containsOnly(oliver, alicia);
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

        List<Person> persons = repository.findByMapOfIntListsContaining(List.of(100), VALUE);
        assertThat(persons).contains(stefan);
        List<Person> persons2 = repository.findByMapOfIntListsContaining(List.of(201), VALUE);
        assertThat(persons2).contains(douglas, matias);
        List<Person> persons3 = repository.findByMapOfIntListsContaining(List.of(20000), VALUE);
        assertThat(persons3).isEmpty();
    }

    @Test
    void findByStringMapKeyValueEquals() {
        assertThat(donny.getStringMap()).containsKey("key1");
        assertThat(donny.getStringMap()).containsValue("val1");
        assertThat(boyd.getStringMap()).containsKey("key1");
        assertThat(boyd.getStringMap()).containsValue("val1");

        List<Person> persons;
        persons = repository.findByStringMapContaining("key1", "val1");
        assertThat(persons).containsExactlyInAnyOrder(donny, boyd);

        persons = repository.findByStringMapContaining("key1", "val1", "key2", "val2");
        assertThat(persons).containsExactly(boyd);

        persons = repository.findByStringMapContaining("key1", "val1", "key2", "val2", "key3", "value3");
        assertThat(persons).isEmpty();
    }

    @Test
    void findByMapOfListsKeyValueEquals() {
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
        persons = repository.findByMapOfIntListsContaining("0", List.of(100));
        assertThat(persons).containsExactlyInAnyOrder(stefan, douglas);

        persons = repository.findByMapOfIntListsContaining("2", List.of(202));
        assertThat(persons).containsExactlyInAnyOrder(matias, leroi2);

        persons = repository.findByMapOfIntListsContaining("34", List.of(2000));
        assertThat(persons).isEmpty();
    }

    @Test
    void findByAddressesMapKeyValueEquals() {
        if (IndexUtils.isFindByPojoSupported(client)) {
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
            persons = repository.findByAddressesMapContaining("a", address1);
            assertThat(persons).containsExactlyInAnyOrder(stefan, douglas, matias, leroi2);

            persons = repository.findByAddressesMapContaining("b", address2);
            assertThat(persons).containsExactly(douglas, leroi2);

            persons = repository.findByAddressesMapContaining("cd", address3);
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
    void findByMapKeyValueNotEqual() {
        assertThat(carter.getIntMap()).containsKey("key1");
        assertThat(!carter.getIntMap().containsValue(22)).isTrue();

        List<Person> persons = repository.findByIntMapIsNot("key1", 22);
        assertThat(persons).containsOnly(carter);
    }

    @Test
    void findByExists() {
        assertThat(stefan.getAddress()).isNull();
        assertThat(carter.getAddress()).isNotNull();
        assertThat(dave.getAddress()).isNotNull();

        assertThat(repository.findByAddressExists()).contains(carter, dave).doesNotContain(stefan);
        assertThat(repository.findByAddressZipCodeExists()).contains(carter, dave).doesNotContain(stefan);

        stefan.setAddress(new Address(null, null, null, null));
        repository.save(stefan);
        // when set to null a bin/field becomes non-existing
        assertThat(repository.findByAddressZipCodeExists()).contains(carter, dave).doesNotContain(stefan);

        stefan.setAddress(new Address(null, null, "zipCode", null));
        repository.save(stefan);
        assertThat(repository.findByAddressZipCodeExists()).contains(carter, dave, stefan);

        Map<String, String> stringMap = new HashMap<>();
        stringMap.put("key", null);
        stefan.setStringMap(stringMap);
        repository.save(stefan);
        assertThat(repository.findByStringMapContaining("key", KEY)).contains(stefan);

        stefan.setAddress(null); // cleanup
        stefan.setStringMap(null);
        repository.save(stefan);
    }

    @Test
    void findByIsNull() {
        assertThat(stefan.getAddress()).isNull();
        assertThat(carter.getAddress()).isNotNull();
        assertThat(dave.getAddress()).isNotNull();
        assertThat(repository.findByAddressIsNull()).contains(stefan).doesNotContain(carter, dave);

        stefan.setAddress(new Address(null, null, null, null));
        repository.save(stefan);
        assertThat(repository.findByAddressIsNull()).doesNotContain(stefan);
        assertThat(repository.findByAddressZipCodeIsNull()).contains(stefan).doesNotContain(carter, dave);

        dave.setBestFriend(stefan);
        repository.save(dave);
        carter.setFriend(dave);
        repository.save(carter);
        assertThat(repository.findByFriendBestFriendAddressZipCodeIsNull()).contains(carter);

        stefan.setAddress(null); // cleanup
        repository.save(stefan);
        TestUtils.setFriendsToNull(repository, carter, dave);

        Map<String, String> stringMap = new HashMap<>();
        stringMap.put("key", null);
        stefan.setStringMap(stringMap);
        repository.save(stefan);
        assertThat(repository.findByStringMapContaining(null, VALUE)).contains(stefan); // among map values

        // Currently getting key-specific results for a Map requires 2 steps:
        // firstly query for all entities with existing map key
        List<Person> personsWithMapKeyExists = repository.findByStringMapContaining("key", KEY);
        // and then leave only the records that have the key's value == null
        List<Person> personsWithMapValueNull = personsWithMapKeyExists.stream()
            .filter(person -> person.getStringMap().get("key") == null).toList();
        assertThat(personsWithMapValueNull).contains(stefan);

        List<String> strings = new ArrayList<>();
        strings.add(null);
        stefan.setStrings(strings);
        repository.save(stefan);
        assertThat(repository.findByStringsContaining(null)).contains(stefan);

        stefan.setStringMap(null); // cleanup
        stefan.setStrings(null);
        repository.save(stefan);
    }

    @Test
    void findByIsNotNull() {
        assertThat(stefan.getAddress()).isNull();
        assertThat(carter.getAddress()).isNotNull();
        assertThat(dave.getAddress()).isNotNull();
        assertThat(repository.findByAddressIsNotNull()).contains(carter, dave).doesNotContain(stefan);

        stefan.setAddress(new Address(null, null, "zipCode", null));
        repository.save(stefan);
        assertThat(repository.findByAddressIsNotNull()).contains(stefan); // Address is not null
        assertThat(repository.findByAddressZipCodeIsNotNull()).contains(stefan); // zipCode is not null

        stefan.setAddress(new Address(null, null, null, null));
        repository.save(stefan);
        assertThat(repository.findByAddressIsNotNull()).contains(stefan); // Address is not null
        assertThat(repository.findByAddressZipCodeIsNotNull()).doesNotContain(stefan); // zipCode is null

        stefan.setAddress(null); // cleanup
        repository.save(stefan);

        Map<String, String> stringMap = new HashMap<>();
        stringMap.put("key", "str");
        stefan.setStringMap(stringMap);
        repository.save(stefan);
        assertThat(repository.findByStringMapNotContaining(null, VALUE)).contains(stefan); // among map values

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
        assertThat(repository.findByStringMapNotContaining(null, VALUE)).doesNotContain(stefan);

        personsWithMapKeyExists = repository.findByStringMapContaining("key", KEY);
        personsWithMapValueNotNull = personsWithMapKeyExists.stream()
            .filter(person -> person.getStringMap().get("key") != null).toList();
        assertThat(personsWithMapValueNotNull).doesNotContain(stefan);

        List<String> strings = new ArrayList<>();
        strings.add("ing");
        stefan.setStrings(strings);
        repository.save(stefan);
        assertThat(repository.findByStringsNotContaining(null)).contains(stefan);

        strings.add(null);
        stefan.setStrings(strings);
        repository.save(stefan);
        assertThat(repository.findByStringsNotContaining(null)).doesNotContain(stefan);

        stefan.setStringMap(null); // cleanup
        stefan.setStrings(null);
        repository.save(stefan);
    }

    @Test
    void findByMapOfListsKeyValueNotEqual() {
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
        persons = repository.findByMapOfIntListsIsNot("2", List.of(100));
        assertThat(persons).containsOnly(matias, leroi2);

        persons = repository.findByMapOfIntListsIsNot("0", List.of(202));
        assertThat(persons).containsOnly(stefan, douglas);

        persons = repository.findByMapOfIntListsIsNot("34", List.of(2000));
        assertThat(persons).isEmpty();
    }

    @Test
    void findByAddressesMapKeyValueNotEqual() {
        if (IndexUtils.isFindByPojoSupported(client)) {
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
            persons = repository.findByAddressesMapIsNot("a", address1);
            assertThat(persons).isEmpty();

            persons = repository.findByAddressesMapIsNot("b", address1);
            assertThat(persons).containsExactlyInAnyOrder(leroi2, douglas);

            persons = repository.findByAddressesMapIsNot("cd", address3);
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
    void findByMapKeyValueContains() {
        assertThat(donny.getStringMap()).containsKey("key1");
        assertThat(donny.getStringMap()).containsValue("val1");
        assertThat(boyd.getStringMap()).containsKey("key1");
        assertThat(boyd.getStringMap()).containsValue("val1");

        List<Person> persons = repository.findByStringMapContaining("key1", "al", VALUE_CONTAINING);
        assertThat(persons).contains(donny, boyd);

        List<Person> persons2 = repository.findByStringMapContaining("key1", "al", "key2", "va", VALUE_CONTAINING);
        assertThat(persons2).contains(boyd);
    }

    @Test
    void findByMapKeyValueStartsWith() {
        assertThat(donny.getStringMap()).containsKey("key1");
        assertThat(donny.getStringMap()).containsValue("val1");
        assertThat(boyd.getStringMap()).containsKey("key1");
        assertThat(boyd.getStringMap()).containsValue("val1");

        List<Person> persons = repository.findByStringMapStartsWith("key1", "val");
        assertThat(persons).contains(donny, boyd);
    }

    @Test
    void findByMapKeyValueLike() {
        assertThat(donny.getStringMap()).containsKey("key1");
        assertThat(donny.getStringMap()).containsValue("val1");
        assertThat(boyd.getStringMap()).containsKey("key1");
        assertThat(boyd.getStringMap()).containsValue("val1");

        List<Person> persons = repository.findByStringMapLike("key1", "^.*al1$");
        assertThat(persons).contains(donny, boyd);
    }

    @Test
    void findByMapKeyValueGreaterThan() {
        assertThat(carter.getIntMap()).containsKey("key2");
        assertThat(carter.getIntMap().get("key2") > 0).isTrue();

        List<Person> persons = repository.findByIntMapGreaterThan("key2", 0);
        assertThat(persons).containsExactly(carter);
    }

    @Test
    void findByMapKeyValueLessThanOrEqual() {
        assertThat(carter.getIntMap()).containsKey("key2");
        assertThat(carter.getIntMap().get("key2") > 0).isTrue();

        List<Person> persons = repository.findByIntMapLessThanEqual("key2", 1);
        assertThat(persons).containsExactly(carter);
    }

    @Test
    void findByMapKeyValueGreaterThanList() {
        if (IndexUtils.isFindByPojoSupported(client)) {
            Map<String, List<Integer>> mapOfLists1 = Map.of("0", List.of(100), "1", List.of(200), "2", List.of(300),
                "3", List.of(400));
            Map<String, List<Integer>> mapOfLists2 = Map.of("0", List.of(101), "1", List.of(201), "2", List.of(301),
                "3", List.of(401));
            Map<String, List<Integer>> mapOfLists3 = Map.of("1", List.of(102), "2", List.of(202), "3", List.of(300),
                "4", List.of(400));
            Map<String, List<Integer>> mapOfLists4 = Map.of("2", List.of(1000), "3", List.of(2000), "4",
                List.of(3000), "5", List.of(4000));
            stefan.setMapOfIntLists(mapOfLists1);
            repository.save(stefan);
            douglas.setMapOfIntLists(mapOfLists2);
            repository.save(douglas);
            matias.setMapOfIntLists(mapOfLists3);
            repository.save(matias);
            leroi2.setMapOfIntLists(mapOfLists4);
            repository.save(leroi2);

            List<Person> persons;
            persons = repository.findByMapOfIntListsGreaterThan("0", List.of(100));
            assertThat(persons).containsOnly(douglas);

            persons = repository.findByMapOfIntListsGreaterThan("1", List.of(102));
            assertThat(persons).containsOnly(stefan, douglas);

            persons = repository.findByMapOfIntListsGreaterThan("2", List.of(200));
            assertThat(persons).containsOnly(stefan, douglas, matias, leroi2);

            persons = repository.findByMapOfIntListsGreaterThan("3", List.of(2000));
            assertThat(persons).isEmpty();
        }
    }

    @Test
    void findByMapKeyValueLessThanAddress() {
        if (IndexUtils.isFindByPojoSupported(client)) {
            Map<String, Address> mapOfAddresses1 = Map.of("a", new Address("Foo Street 1", 1, "C0123", "Bar"));
            Map<String, Address> mapOfAddresses2 = Map.of("b", new Address("Foo Street 2", 1, "C0123", "Bar"));
            Map<String, Address> mapOfAddresses3 = Map.of("c", new Address("Foo Street 2", 1, "C0124", "Bar"));
            Map<String, Address> mapOfAddresses4 = Map.of("d", new Address("Foo Street 1234", 1, "C01245", "Bar"));
            stefan.setAddressesMap(mapOfAddresses1);
            repository.save(stefan);
            douglas.setAddressesMap(mapOfAddresses2);
            repository.save(douglas);
            matias.setAddressesMap(mapOfAddresses3);
            repository.save(matias);
            leroi2.setAddressesMap(mapOfAddresses4);
            repository.save(leroi2);

            List<Person> persons;
            persons = repository.findByAddressesMapLessThan("a", new Address("Foo Street 1", 1, "C0124", "Bar"));
            assertThat(persons).containsOnly(stefan);

            persons = repository.findByAddressesMapLessThan("b", new Address("Foo Street 3", 1, "C0123", "Bar"));
            assertThat(persons).containsOnly(douglas);

            persons = repository.findByAddressesMapLessThan("c", new Address("Foo Street 3", 2, "C0124", "Bar"));
            assertThat(persons).containsOnly(matias);

            persons = repository.findByAddressesMapLessThan("d", new Address("Foo Street 1234", 1, "C01245", "Bar"));
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
    void findByIntMapKeyValueBetween() {
        assertThat(carter.getIntMap()).containsKey("key1");
        assertThat(carter.getIntMap().get("key1") >= 0).isTrue();

        List<Person> persons;
        persons = repository.findByIntMapBetween("key1", 0, 1);
        assertThat(persons).contains(carter);

        assertThat(donny.getStringMap()).containsKey("key1");
        assertThat(boyd.getStringMap()).containsKey("key1");
        assertThat(donny.getStringMap().get("key1").equals("val1")).isTrue();
        assertThat(boyd.getStringMap().get("key1").equals("val1")).isTrue();

        persons = repository.findByStringMapBetween("key1", "val1", "val2");
        assertThat(persons).contains(boyd, donny);
    }

    @Test
    void findByIntMapBetween() {
        if (IndexUtils.isFindByPojoSupported(client)) {
            assertThat(carter.getIntMap()).isEqualTo(Map.of("key1", 0, "key2", 1));

            Map<String, Integer> map1 = Map.of("key1", -1, "key2", 0);
            Map<String, Integer> map2 = Map.of("key1", 2, "key2", 3);

            List<Person> persons;
            persons = repository.findByIntMapBetween(map1, map2);
            assertThat(persons).contains(carter);
        }
    }

    @Test
    void findByMapOfListsBetween() {
        if (IndexUtils.isFindByPojoSupported(client)) {
            Map<String, List<Integer>> mapOfLists1 = Map.of("0", List.of(100), "1", List.of(200));
            Map<String, List<Integer>> mapOfLists2 = Map.of("2", List.of(301), "3", List.of(401));
            Map<String, List<Integer>> mapOfLists3 = Map.of("1", List.of(102), "2", List.of(202));
            Map<String, List<Integer>> mapOfLists4 = Map.of("3", List.of(3000), "4", List.of(4000));
            stefan.setMapOfIntLists(mapOfLists1);
            repository.save(stefan);
            douglas.setMapOfIntLists(mapOfLists2);
            repository.save(douglas);
            matias.setMapOfIntLists(mapOfLists3);
            repository.save(matias);
            leroi2.setMapOfIntLists(mapOfLists4);
            repository.save(leroi2);

            List<Person> persons;
            persons = repository.findByMapOfIntListsBetween(Map.of("0", List.of(100), "1", List.of(200)),
                Map.of("3", List.of(3000), "4", List.of(4001)));
            assertThat(persons).contains(stefan, douglas, matias, leroi2);

            persons = repository.findByMapOfIntListsBetween(Map.of("0", List.of(100), "1", List.of(200)),
                Map.of("3", List.of(3000), "4", List.of(4000)));
            assertThat(persons).contains(stefan, douglas, matias);

            persons = repository.findByMapOfIntListsBetween(Map.of("5", List.of(4001)), Map.of("910", List.of(10000)));
            assertThat(persons).isEmpty();
        }
    }

    @Test
    void findByRegDateBefore() {
        dave.setRegDate(LocalDate.of(1980, 3, 10));
        repository.save(dave);

        List<Person> persons = repository.findByRegDateBefore(LocalDate.of(1981, 3, 10));
        assertThat(persons).contains(dave);

        dave.setDateOfBirth(null);
        repository.save(dave);
    }

    @Test
    void findByDateOfBirthAfter() {
        dave.setDateOfBirth(new Date());
        repository.save(dave);

        List<Person> persons = repository.findByDateOfBirthAfter(new Date(126230400));
        assertThat(persons).contains(dave);

        dave.setDateOfBirth(null);
        repository.save(dave);
    }

    @Test
    void findByRegDate() {
        LocalDate date = LocalDate.of(1970, 3, 10);
        carter.setRegDate(date);
        repository.save(carter);

        List<Person> persons = repository.findByRegDate(date);
        assertThat(persons).contains(carter);

        carter.setRegDate(null);
        repository.save(carter);
    }

    @Test
    void findByFirstNameContaining() {
        List<Person> persons = repository.findByFirstNameContaining("er");
        assertThat(persons).containsExactlyInAnyOrder(carter, oliver, leroi, leroi2);
    }

    @Test
    void findByFirstNameNotContaining() {
        List<Person> persons = repository.findByFirstNameNotContaining("er");
        assertThat(persons).containsExactlyInAnyOrder(dave, donny, alicia, boyd, stefan, matias, douglas);
    }

    @Test
    void findByFirstNameLike() { // with a wildcard
        List<Person> persons = repository.findByFirstNameLike("Ca.*er");
        assertThat(persons).contains(carter);

        List<Person> persons0 = repository.findByFirstNameLikeIgnoreCase("CART.*er");
        assertThat(persons0).contains(carter);

        List<Person> persons1 = repository.findByFirstNameLike(".*ve.*");
        assertThat(persons1).contains(dave, oliver);

        List<Person> persons2 = repository.findByFirstNameLike("Carr.*er");
        assertThat(persons2).isEmpty();
    }

    @Test
    void findByAddressZipCodeContaining() {
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
    void findByAddressZipCodeNotContaining() {
        Address cartersAddress = carter.getAddress();
        Address davesAddress = dave.getAddress();

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
    }

    @Test
    public void findPersonById() {
        Optional<Person> person = repository.findById(dave.getId());

        assertThat(person).hasValueSatisfying(actual -> {
            assertThat(actual).isInstanceOf(Person.class);
            assertThat(actual).isEqualTo(dave);
        });
    }

    @Test
    public void findAll() {
        List<Person> result = (List<Person>) repository.findAll();
        assertThat(result).containsExactlyInAnyOrderElementsOf(allPersons);
    }

    @Test
    public void findAllWithGivenIds() {
        List<Person> result = (List<Person>) repository.findAllById(List.of(dave.getId(), boyd.getId()));

        assertThat(result)
            .hasSize(2)
            .contains(dave)
            .doesNotContain(oliver, carter, alicia);
    }

    @Test
    public void findPersonsByLastName() {
        List<Person> result = repository.findByLastName("Beauford");

        assertThat(result)
            .hasSize(1)
            .containsOnly(carter);
    }

    @Test
    public void findPersonsSomeFieldsByLastNameProjection() {
        List<PersonSomeFields> result = repository.findPersonSomeFieldsByLastName("Beauford");

        assertThat(result)
            .hasSize(1)
            .containsOnly(carter.toPersonSomeFields());
    }

    @Test
    public void findDynamicTypeByLastNameDynamicProjection() {
        List<PersonSomeFields> result = repository.findByLastName("Beauford", PersonSomeFields.class);

        assertThat(result)
            .hasSize(1)
            .containsOnly(carter.toPersonSomeFields());
    }

    @Test
    public void findPersonsByFriendAge() {
        oliver.setFriend(alicia);
        repository.save(oliver);
        dave.setFriend(oliver);
        repository.save(dave);
        carter.setFriend(dave);
        repository.save(carter);
        assertThat(dave.getAge()).isEqualTo(42);

        List<Person> result = repository.findByFriendAge(42);

        assertThat(result)
            .hasSize(1)
            .containsExactly(carter);

        TestUtils.setFriendsToNull(repository, oliver, dave, carter);
    }

    @Test
    public void findPersonsByFriendAgeNotEqual() {
        oliver.setFriend(alicia);
        repository.save(oliver);
        dave.setFriend(oliver);
        repository.save(dave);
        carter.setFriend(dave);
        repository.save(carter);

        List<Person> result = repository.findByFriendAgeIsNot(42);

        assertThat(result)
            .hasSize(2)
            .containsExactlyInAnyOrder(dave, oliver);

        TestUtils.setFriendsToNull(repository, oliver, dave, carter);
    }

    @Test
    public void findPersonsByAddressZipCode() {
        Address cartersAddress = carter.getAddress();
        Address davesAddress = dave.getAddress();

        String zipCode = "C0123456";
        carter.setAddress(new Address("Foo Street 2", 2, "C012344", "C0123"));
        repository.save(carter);
        dave.setAddress(new Address("Foo Street 1", 1, zipCode, "Bar"));
        repository.save(dave);
        boyd.setAddress(new Address(null, null, null, null));
        repository.save(boyd);

        List<Person> result = repository.findByAddressZipCode(zipCode);

        assertThat(result)
            .hasSize(1)
            .containsExactly(dave);

        carter.setAddress(cartersAddress);
        repository.save(carter);
        dave.setAddress(davesAddress);
        repository.save(dave);
    }

    @Test
    public void findPersonsByFriendAgeGreaterThan() {
        alicia.setFriend(boyd);
        repository.save(alicia);
        dave.setFriend(oliver);
        repository.save(dave);
        carter.setFriend(dave);
        repository.save(carter);
        leroi.setFriend(carter);
        repository.save(leroi);

        assertThat(alicia.getFriend().getAge()).isGreaterThan(42);
        assertThat(leroi.getFriend().getAge()).isGreaterThan(42);

        List<Person> result = repository.findByFriendAgeGreaterThan(42);

        assertThat(result)
            .hasSize(2)
            .containsExactlyInAnyOrder(alicia, leroi);

        TestUtils.setFriendsToNull(repository, alicia, dave, carter, leroi);
    }

    @Test
    public void findPersonsByFriendAgeLessThanOrEqual() {
        alicia.setFriend(boyd);
        repository.save(alicia);
        dave.setFriend(oliver);
        repository.save(dave);
        carter.setFriend(dave);
        repository.save(carter);
        leroi.setFriend(carter);
        repository.save(leroi);

        List<Person> result = repository.findByFriendAgeLessThanEqual(42);

        assertThat(result)
            .hasSize(2)
            .containsExactlyInAnyOrder(dave, carter);

        TestUtils.setFriendsToNull(repository, alicia, dave, carter, leroi);
    }

    @Test
    public void findAll_doesNotFindDeletedPersonByEntity() {
        try {
            repository.delete(dave);
            List<Person> result = (List<Person>) repository.findAll();
            assertThat(result)
                .doesNotContain(dave)
                .containsExactlyInAnyOrderElementsOf(
                    allPersons.stream().filter(person -> !person.equals(dave)).collect(Collectors.toList())
                );
        } finally {
            repository.save(dave);
        }
    }

    @Test
    public void findAll_doesNotFindDeletedPersonById() {
        try {
            repository.deleteById(dave.getId());
            List<Person> result = (List<Person>) repository.findAll();
            assertThat(result)
                .doesNotContain(dave)
                .hasSize(allPersons.size() - 1);
        } finally {
            repository.save(dave);
        }
    }

    @Test
    public void findPersonsByFirstName() {
        List<Person> result = repository.findByFirstName("Leroi");
        assertThat(result).hasSize(2).containsOnly(leroi, leroi2);

        List<Person> result1 = repository.findByFirstNameIgnoreCase("lEroi");
        assertThat(result1).hasSize(2).containsOnly(leroi, leroi2);

        List<Person> result2 = repository.findByFirstName("lEroi");
        assertThat(result2).hasSize(0);
    }

    @Test
    public void findPersonsByFirstNameNot() {
        List<Person> result = repository.findByFirstNameNot("Leroi");
        assertThat(result).doesNotContain(leroi, leroi2);

        List<Person> result1 = repository.findByFirstNameNotIgnoreCase("lEroi");
        assertThat(result1).doesNotContain(leroi, leroi2);

        List<Person> result2 = repository.findByFirstNameNot("lEroi");
        assertThat(result2).contains(leroi, leroi2);
    }

    @Test
    public void findPersonsByFirstNameGreaterThan() {
        List<Person> result = repository.findByFirstNameGreaterThan("Leroa");
        assertThat(result).contains(leroi, leroi2);
    }

    @Test
    public void findByLastNameNot_forExistingResult() {
        Stream<Person> result = repository.findByLastNameNot("Moore");

        assertThat(result)
            .doesNotContain(leroi, leroi2)
            .contains(dave, donny, oliver, carter, boyd, stefan, alicia);
    }

    @Test
    public void findByFirstNameIn() {
        Stream<Person> result;
        result = repository.findByFirstNameIn(List.of("Anastasiia", "Daniil"));
        assertThat(result).isEmpty();

        result = repository.findByFirstNameIn(List.of("Alicia", "Stefan"));
        assertThat(result).contains(alicia, stefan);
    }

    @Test
    public void findByFirstNameNotIn() {
        Collection<String> firstNames;
        firstNames = allPersons.stream().map(Person::getFirstName).collect(Collectors.toSet());
        assertThat(repository.findByFirstNameNotIn(firstNames)).isEmpty();

        firstNames = List.of("Dave", "Donny", "Carter", "Boyd", "Leroi", "Stefan", "Matias", "Douglas");
        assertThat(repository.findByFirstNameNotIn(firstNames)).containsExactlyInAnyOrder(oliver, alicia);
    }

    @Test
    public void countByLastName_forExistingResult() {
        assertThatThrownBy(() -> repository.countByLastName("Leroi"))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Query method Person.countByLastName not supported.");

//		assertThat(result).isEqualTo(2);
    }

    @Test
    public void countByLastName_forEmptyResult() {
        assertThatThrownBy(() -> repository.countByLastName("Smirnova"))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Query method Person.countByLastName not supported.");

//		assertThat(result).isEqualTo(0);
    }

    @Test
    public void findByAgeGreaterThan_forExistingResult() {
        Slice<Person> slice = repository.findByAgeGreaterThan(40, PageRequest.of(0, 10));

        assertThat(slice.hasContent()).isTrue();
        assertThat(slice.hasNext()).isFalse();
        assertThat(slice.getContent()).hasSize(4).contains(dave, carter, boyd, leroi);
    }

    @Test
    public void findPersonsSomeFieldsByAgeGreaterThan_forExistingResultProjection() {
        Slice<PersonSomeFields> slice = repository.findPersonSomeFieldsByAgeGreaterThan(
            40, PageRequest.of(0, 10)
        );

        assertThat(slice.hasContent()).isTrue();
        assertThat(slice.hasNext()).isFalse();
        assertThat(slice.getContent()).hasSize(4).contains(dave.toPersonSomeFields(),
            carter.toPersonSomeFields(), boyd.toPersonSomeFields(), leroi.toPersonSomeFields());
    }

    @Test
    public void findByAgeGreaterThan_respectsLimit() {
        Slice<Person> slice = repository.findByAgeGreaterThan(40, PageRequest.of(0, 1));

        assertThat(slice.hasContent()).isTrue();
        assertThat(slice.hasNext()).isTrue();
        assertThat(slice.getContent()).containsAnyOf(dave, carter, boyd, leroi).hasSize(1);
    }

    @Test
    public void findByAgeGreaterThan_respectsLimitAndOffsetAndSort() {
        List<Person> result = IntStream.range(0, 4)
            .mapToObj(index -> repository.findByAgeGreaterThan(40, PageRequest.of(
                index, 1, Sort.by("age")
            )))
            .flatMap(slice -> slice.getContent().stream())
            .collect(Collectors.toList());

        assertThat(result)
            .hasSize(4)
            .containsSequence(dave, leroi, boyd, carter);
    }

    @Test
    public void findByAgeGreaterThan_returnsValidValuesForNextAndPrev() {
        Slice<Person> first = repository.findByAgeGreaterThan(40, PageRequest.of(0, 1, Sort.by("age")));
        assertThat(first.hasContent()).isTrue();
        assertThat(first.getNumberOfElements()).isEqualTo(1);
        assertThat(first.hasNext()).isTrue();
        assertThat(first.isFirst()).isTrue();
        assertThat(first.isLast()).isFalse();

        Slice<Person> last = repository.findByAgeGreaterThan(40, PageRequest.of(3, 1, Sort.by("age")));
        assertThat(last.hasContent()).isTrue();
        assertThat(last.getNumberOfElements()).isEqualTo(1);
        assertThat(last.hasNext()).isFalse();
        assertThat(last.isLast()).isTrue();
    }

    @Test
    public void findByAgeGreaterThan_forEmptyResult() {
        Slice<Person> slice = repository.findByAgeGreaterThan(100, PageRequest.of(0, 10));

        assertThat(slice.hasContent()).isFalse();
        assertThat(slice.hasNext()).isFalse();
        assertThat(slice.getContent()).isEmpty();
    }

    @Test
    public void findByLastNameStartsWithOrderByAgeAsc_respectsLimitAndOffset() {
        Page<Person> first = repository.findByLastNameStartsWithOrderByAgeAsc("Mo", PageRequest.of(0, 1));

        assertThat(first.getNumberOfElements()).isEqualTo(1);
        assertThat(first.getTotalPages()).isEqualTo(2);
        assertThat(first.get()).hasSize(1).containsOnly(leroi2);

        Page<Person> last = repository.findByLastNameStartsWithOrderByAgeAsc("Mo", first.nextPageable());

        assertThat(last.getTotalPages()).isEqualTo(2);
        assertThat(last.getNumberOfElements()).isEqualTo(1);
        assertThat(last.get()).hasSize(1).containsAnyOf(leroi);

        Page<Person> all = repository.findByLastNameStartsWithOrderByAgeAsc("Mo", PageRequest.of(0, 5));

        assertThat(all.getTotalPages()).isEqualTo(1);
        assertThat(all.getNumberOfElements()).isEqualTo(2);
        assertThat(all.get()).hasSize(2).containsOnly(leroi, leroi2);
    }

    @Test
    public void findByLastNameStartingWith_limited() {
        Person person = repository.findFirstByLastNameStartingWith("M", Sort.by("lastName").ascending());
        assertThat(person).isEqualTo(donny);

        List<Person> personList = repository.findTopByLastNameStartingWith("M", Sort.by("lastName").ascending());
        assertThat(personList).hasSize(1);
        assertThat(personList.get(0)).isEqualTo(person);

        Person person2 = repository.findFirstByLastNameStartingWith("M", Sort.by("age").descending());
        assertThat(person2).isEqualTo(leroi);

        List<Person> persons = repository.findTop3ByLastNameStartingWith("M", Sort.by("lastName", "firstName")
            .ascending());
        List<Person> persons2 = repository.findFirst3ByLastNameStartingWith("M", Sort.by("lastName", "firstName")
            .ascending());
        assertThat(persons).hasSize(3).containsExactly(donny, dave, oliver).isEqualTo(persons2);

        Page<Person> personsPage = repository.findTop3ByLastNameStartingWith("M",
            PageRequest.of(0, 3, Sort.by("lastName", "firstName").ascending()));
        assertThat(personsPage.get()).containsExactly(donny, dave, oliver);
    }

    @Test
    public void findPersonsByFirstNameAndByAge() {
        List<Person> result = repository.findByFirstNameAndAge("Leroi", 25);
        assertThat(result).containsOnly(leroi2);

        result = repository.findByFirstNameAndAge("Leroi", 44);
        assertThat(result).containsOnly(leroi);
    }

    @Test
    public void findPersonsByFirstNameStartsWith() {
        List<Person> result = repository.findByFirstNameStartsWith("D");

        assertThat(result).containsOnly(dave, donny, douglas);
    }

    @Test
    public void findPersonsByFriendFirstNameStartsWith() {
        stefan.setFriend(oliver);
        repository.save(stefan);
        carter.setFriend(dave);
        repository.save(carter);

        List<Person> result = repository.findByFriendFirstNameStartsWith("D");
        assertThat(result)
            .hasSize(1)
            .containsExactly(carter);

        TestUtils.setFriendsToNull(repository, stefan, carter);
    }

    @Test
    public void findPersonsByFriendLastNameLike() {
        oliver.setFriend(dave);
        repository.save(oliver);
        carter.setFriend(stefan);
        repository.save(carter);

        List<Person> result = repository.findByFriendLastNameLike(".*tthe.*");
        assertThat(result).contains(oliver);
        TestUtils.setFriendsToNull(repository, oliver, carter);
    }

    @Test
    public void findPagedPersons() {
        Page<Person> result = repository.findAll(PageRequest.of(
            1, 2, Sort.Direction.ASC, "lastname", "firstname")
        );
        assertThat(result.isFirst()).isFalse();
        assertThat(result.isLast()).isFalse();
    }

    @Test
    public void findPersonInRangeCorrectly() {
        Iterable<Person> it;
        it = repository.findByAgeBetween(40, 46);
        assertThat(it).hasSize(3).contains(dave);

        it = repository.findByFirstNameBetween("Dave", "David");
        assertThat(it).hasSize(1).contains(dave);

        if (IndexUtils.isFindByPojoSupported(client)) {
            assertThat(dave.getAddress()).isEqualTo(new Address("Foo Street 1", 1, "C0123", "Bar"));
            Address address1 = new Address("Foo Street 1", 0, "C0123", "Bar");
            Address address2 = new Address("Foo Street 2", 2, "C0124", "Bar");
            it = repository.findByAddressBetween(address1, address2);
            assertThat(it).hasSize(1).contains(dave);

            address1 = new Address("Foo Street 0", 0, "C0122", "Bar");
            address2 = new Address("Foo Street 0", 0, "C0123", "Bar");
            it = repository.findByAddressBetween(address1, address2);
            assertThat(it).isEmpty();
        }
    }

    @Test
    public void findPersonInAgeRangeCorrectlyOrderByLastName() {
        Iterable<Person> it = repository.findByAgeBetweenOrderByLastName(30, 46);
        assertThat(it).hasSize(6);
    }

    @Test
    public void findPersonInAgeRangeAndNameCorrectly() {
        Iterable<Person> it = repository.findByAgeBetweenAndLastName(40, 45, "Matthews");
        assertThat(it).hasSize(1);

        Iterable<Person> result = repository.findByAgeBetweenAndLastName(20, 26, "Moore");
        assertThat(result).hasSize(1);
    }

    @Test
    public void findPersonsByFriendsInAgeRangeCorrectly() {
        oliver.setFriend(alicia);
        repository.save(oliver);
        dave.setFriend(oliver);
        repository.save(dave);
        carter.setFriend(dave);
        repository.save(carter);

        List<Person> result = repository.findByFriendAgeBetween(40, 45);
        assertThat(result)
            .hasSize(1)
            .containsExactly(carter);

        TestUtils.setFriendsToNull(repository, oliver, dave, carter);
    }

    @Test
    public void findPersonsByStringsList() {
        if (IndexUtils.isFindByPojoSupported(client)) {
            List<String> listToCompareWith = List.of("str0", "str1", "str2");
            assertThat(dave.getStrings()).isEqualTo(listToCompareWith);

            List<Person> persons = repository.findByStringsEquals(listToCompareWith);
            assertThat(persons).contains(dave);

            // another way to call the method
            List<Person> persons2 = repository.findByStrings(listToCompareWith);
            assertThat(persons2).contains(dave);
        }
    }

    @Test
    public void findPersonsByStringsListNotEqual() {
        if (IndexUtils.isFindByPojoSupported(client)) {
            List<String> listToCompareWith = List.of("str0", "str1", "str2");
            assertThat(dave.getStrings()).isEqualTo(listToCompareWith);
            assertThat(donny.getStrings()).isNotEmpty();
            assertThat(donny.getStrings()).isNotEqualTo(listToCompareWith);

            List<Person> persons = repository.findByStringsIsNot(listToCompareWith);
            assertThat(persons).contains(donny);
        }
    }

    @Test
    public void findPersonsByStringsListLessThan() {
        if (IndexUtils.isFindByPojoSupported(client)) {
            List<String> davesStrings = dave.getStrings();
            List<String> listToCompareWith = List.of("str1", "str2", "str3");
            List<String> listWithFewerElements = List.of("str1", "str2");

            dave.setStrings(listWithFewerElements);
            repository.save(dave);
            assertThat(donny.getStrings()).isEqualTo(listToCompareWith);
            assertThat(dave.getStrings()).isEqualTo(listWithFewerElements);

            List<Person> persons = repository.findByStringsLessThan(listToCompareWith);
            assertThat(persons).contains(dave);

            dave.setStrings(davesStrings);
            repository.save(dave);
        }
    }

    @Test
    public void findPersonsByStringsListGreaterThanOrEqual() {
        if (IndexUtils.isFindByPojoSupported(client)) {
            Set<Integer> setToCompareWith = Set.of(0, 1, 2, 3, 4);
            dave.setIntSet(setToCompareWith);
            repository.save(dave);
            assertThat(dave.getIntSet()).isEqualTo(setToCompareWith);

            List<Person> persons = repository.findByIntSetGreaterThanEqual(setToCompareWith);
            assertThat(persons).contains(dave);
        }
    }

    @Test
    public void findPersonsByStringMap() {
        if (IndexUtils.isFindByPojoSupported(client)) {
            Map<String, String> mapToCompareWith = Map.of("key1", "val1", "key2", "val2");
            assertThat(boyd.getStringMap()).isEqualTo(mapToCompareWith);

            List<Person> persons = repository.findByStringMapEquals(mapToCompareWith);
            assertThat(persons).contains(boyd);

            // another way to call the method
            List<Person> persons2 = repository.findByStringMap(mapToCompareWith);
            assertThat(persons2).contains(boyd);
        }
    }

    @Test
    public void findPersonsByAddress() {
        if (IndexUtils.isFindByPojoSupported(client)) {
            Address address = new Address("Foo Street 1", 1, "C0123", "Bar");
            dave.setAddress(address);
            repository.save(dave);

            List<Person> persons = repository.findByAddress(address);
            assertThat(persons).containsOnly(dave);
        }
    }

    @Test
    public void findPersonsByAddressNotEqual() {
        if (IndexUtils.isFindByPojoSupported(client)) {
            Address address = new Address("Foo Street 1", 1, "C0123", "Bar");
            assertThat(dave.getAddress()).isEqualTo(address);
            assertThat(carter.getAddress()).isNotNull();
            assertThat(carter.getAddress()).isNotEqualTo(address);
            assertThat(boyd.getAddress()).isNotNull();
            assertThat(boyd.getAddress()).isNotEqualTo(address);

            List<Person> persons = repository.findByAddressIsNot(address);
            assertThat(persons).contains(carter, boyd);
        }
    }

    @Test
    public void findPersonsByIntMapNotEqual() {
        if (IndexUtils.isFindByPojoSupported(client)) {
            Map<String, Integer> mapToCompareWith = Map.of("key1", 0, "key2", 1);
            assertThat(carter.getIntMap()).isEqualTo(mapToCompareWith);
            assertThat(boyd.getIntMap()).isNullOrEmpty();

            carter.setIntMap(Map.of("key1", 1, "key2", 2));
            repository.save(carter);
            assertThat(carter.getIntMap()).isNotEqualTo(mapToCompareWith);

            assertThat(repository.findByIntMapIsNot(mapToCompareWith)).contains(carter);

            carter.setIntMap(mapToCompareWith);
            repository.save(carter);
        }
    }

    @Test
    public void findPersonsByAddressLessThan() {
        if (IndexUtils.isFindByPojoSupported(client)) {
            Address address = new Address("Foo Street 2", 2, "C0124", "C0123");
            assertThat(dave.getAddress()).isNotEqualTo(address);
            assertThat(carter.getAddress()).isEqualTo(address);

            List<Person> persons = repository.findByAddressLessThan(address);
            assertThat(persons).containsExactlyInAnyOrder(dave, boyd);
        }
    }

    @Test
    public void findPersonsByStringMapGreaterThan() {
        if (IndexUtils.isFindByPojoSupported(client)) {
            assertThat(boyd.getStringMap()).isNotEmpty();
            assertThat(donny.getStringMap()).isNotEmpty();

            Map<String, String> mapToCompare = Map.of("Key", "Val", "Key2", "Val2");
            List<Person> persons = repository.findByStringMapGreaterThan(mapToCompare);
            assertThat(persons).containsExactlyInAnyOrder(boyd);
        }
    }

    @Test
    public void findPersonsByFriend() {
        if (IndexUtils.isFindByPojoSupported(client)) {
            alicia.setAddress(new Address("Foo Street 1", 1, "C0123", "Bar"));
            repository.save(alicia);
            oliver.setFriend(alicia);
            repository.save(oliver);

            List<Person> persons = repository.findByFriend(alicia);
            assertThat(persons).containsOnly(oliver);

            alicia.setAddress(null);
            repository.save(alicia);
        }
    }

    @Test
    public void findPersonsByFriendAddress() {
        if (IndexUtils.isFindByPojoSupported(client)) {
            Address address = new Address("Foo Street 1", 1, "C0123", "Bar");
            dave.setAddress(address);
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendAddress(address);

            assertThat(result)
                .hasSize(1)
                .containsExactly(carter);

            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    public void findPersonsByFriendAddressZipCode() {
        String zipCode = "C012345";
        Address address = new Address("Foo Street 1", 1, zipCode, "Bar");
        dave.setAddress(address);
        repository.save(dave);

        carter.setFriend(dave);
        repository.save(carter);

        List<Person> result = repository.findByFriendAddressZipCode(zipCode);

        assertThat(result)
            .hasSize(1)
            .containsExactly(carter);

        TestUtils.setFriendsToNull(repository, carter);
    }

    @Test
    public void findPersonsByFriendFriendAddressZipCode() {
        String zipCode = "C0123";
        Address address = new Address("Foo Street 1", 1, zipCode, "Bar");
        dave.setAddress(address);
        repository.save(dave);

        carter.setFriend(dave);
        repository.save(carter);
        oliver.setFriend(carter);
        repository.save(oliver);

        List<Person> result = repository.findByFriendFriendAddressZipCode(zipCode);

        assertThat(result)
            .hasSize(1)
            .containsExactly(oliver);

        TestUtils.setFriendsToNull(repository, carter, oliver);
    }

    @Test
    // find by deeply nested String POJO field
    public void findPersonsByFriendFriendFriendFriendFriendFriendFriendFriendBestFriendFriendAddressZipCode() {
        String zipCode = "C0123";
        Address address = new Address("Foo Street 1", 1, zipCode, "Bar");
        dave.setAddress(address);
        repository.save(dave);

        alicia.setFriend(dave);
        repository.save(alicia);
        oliver.setBestFriend(alicia);
        repository.save(oliver);
        carter.setFriend(oliver);
        repository.save(carter);
        donny.setFriend(carter);
        repository.save(donny);
        boyd.setFriend(donny);
        repository.save(boyd);
        stefan.setFriend(boyd);
        repository.save(stefan);
        leroi.setFriend(stefan);
        repository.save(leroi);
        leroi2.setFriend(leroi);
        repository.save(leroi2);
        matias.setFriend(leroi2);
        repository.save(matias);
        douglas.setFriend(matias);
        repository.save(douglas);

        List<Person> result =
            repository.findByFriendFriendFriendFriendFriendFriendFriendFriendBestFriendFriendAddressZipCode(zipCode);

        assertThat(result)
            .hasSize(1)
            .containsExactly(douglas);

        TestUtils.setFriendsToNull(repository, allPersons.toArray(Person[]::new));
    }

    @Test
    // find by deeply nested Integer POJO field
    public void findPersonsByFriendFriendFriendFriendFriendFriendFriendFriendBestFriendAddressApartmentNumber() {
        int apartment = 10;
        Address address = new Address("Foo Street 1", apartment, "C0123", "Bar");
        alicia.setAddress(address);
        repository.save(alicia);

        oliver.setBestFriend(alicia);
        repository.save(oliver);
        carter.setFriend(oliver);
        repository.save(carter);
        donny.setFriend(carter);
        repository.save(donny);
        boyd.setFriend(donny);
        repository.save(boyd);
        stefan.setFriend(boyd);
        repository.save(stefan);
        leroi.setFriend(stefan);
        repository.save(leroi);
        leroi2.setFriend(leroi);
        repository.save(leroi2);
        douglas.setFriend(leroi2);
        repository.save(douglas);
        matias.setFriend(douglas);
        repository.save(matias);

        List<Person> result =
            repository.findByFriendFriendFriendFriendFriendFriendFriendFriendBestFriendAddressApartment(apartment);

        assertThat(result)
            .hasSize(1)
            .containsExactly(matias);

        TestUtils.setFriendsToNull(repository, allPersons.toArray(Person[]::new));
        alicia.setAddress(null);
        repository.save(alicia);
    }

    @Test
    // find by deeply nested POJO
    public void findPersonsByFriendFriendFriendFriendFriendFriendFriendFriendBestFriendBestFriendAddress() {
        if (IndexUtils.isFindByPojoSupported(client)) {
            Address address = new Address("Foo Street 1", 1, "C0123", "Bar");
            dave.setAddress(address);
            repository.save(dave);

            alicia.setBestFriend(dave);
            repository.save(alicia);
            oliver.setBestFriend(alicia);
            repository.save(oliver);
            carter.setFriend(oliver);
            repository.save(carter);
            donny.setFriend(carter);
            repository.save(donny);
            boyd.setFriend(donny);
            repository.save(boyd);
            stefan.setFriend(boyd);
            repository.save(stefan);
            leroi.setFriend(stefan);
            repository.save(leroi);
            matias.setFriend(leroi);
            repository.save(matias);
            douglas.setFriend(matias);
            repository.save(douglas);
            leroi2.setFriend(douglas);
            repository.save(leroi2);

            List<Person> result =
                repository.findByFriendFriendFriendFriendFriendFriendFriendFriendBestFriendBestFriendAddress(address);

            assertThat(result)
                .hasSize(1)
                .containsExactly(leroi2);

            TestUtils.setFriendsToNull(repository, allPersons.toArray(Person[]::new));
        }
    }
}
