package org.springframework.data.aerospike.repository.query.blocking.find;

import com.aerospike.client.query.IndexType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.repository.query.blocking.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.PersonId;
import org.springframework.data.aerospike.sample.PersonSomeFields;
import org.springframework.data.aerospike.util.TestUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.query.QueryParam.of;

/**
 * Tests for the "Equals" repository query. Keywords: Is, Equals (or no keyword).
 */
public class EqualsTests extends PersonRepositoryQueryTests {

    @Test
    void findBySimplePropertyEquals_String() {
        List<Person> result = repository.findByFirstName("Leroi");
        assertThat(result).containsOnly(leroi, leroi2);

        List<Person> result1 = repository.findByFirstNameIgnoreCase("lEroi");
        assertThat(result1).containsOnly(leroi, leroi2);

        List<Person> result2 = repository.findByFirstNameIs("lEroi"); // another way to call the query method
        assertThat(result2).hasSize(0);

        List<Person> result3 = repository.findByFirstNameEquals("leroi "); // another way to call the query method
        assertThat(result3).hasSize(0);
    }

    @Test
    void findBySimpleProperty_String_projection() {
        List<PersonSomeFields> result = repository.findPersonSomeFieldsByLastName("Beauford");
        assertThat(result).containsOnly(carter.toPersonSomeFields());
    }

    @Test
    void findBySimpleProperty_id_projection() {
        List<PersonId> result = repository.findPersonIdByFirstName("Carter");
        assertThat(result).containsOnly(carter.toPersonId());
    }

    @Test
    void findBySimplePropertyEquals_String_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByFirstName(100))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.firstName EQ: Type mismatch, expecting String");

        assertThatThrownBy(() -> negativeTestsRepository.findByLastName("Beauford", "Boford"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.lastName EQ: invalid number of arguments, expecting one");
    }

    @Test
    void findBySimplePropertyEquals_Enum() {
        List<Person> result = repository.findByGender(Person.Gender.FEMALE);
        assertThat(result).containsOnly(alicia);
    }

    @Test
    void findBySimplePropertyEquals_Boolean() {
        Person intBoolBinPerson = Person.builder().id(BaseIntegrationTests.nextId()).isActive(true).firstName("Test")
            .build();
        repository.save(intBoolBinPerson);

        List<Person> persons = repository.findByIsActive(true);
        assertThat(persons).contains(intBoolBinPerson);

        repository.delete(intBoolBinPerson);
    }

    @Test
    void findBySimplePropertyEquals_Boolean_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByIsActive())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.isActive EQ: invalid number of arguments, expecting one");

        assertThatThrownBy(() -> negativeTestsRepository.findByIsActive(true, false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.isActive EQ: invalid number of arguments, expecting one");
    }

    @Test
    void findById() {
        Optional<Person> result = repository.findById(dave.getId());
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(dave);
    }

    @Test
    void findById_AND_simpleProperty() {
        QueryParam ids = of(dave.getId());
        QueryParam name = of(carter.getFirstName());
        List<Person> persons = repository.findByIdAndFirstName(ids, name);
        assertThat(persons).isEmpty();

        ids = of(dave.getId());
        name = of(dave.getFirstName());
        persons = repository.findByIdAndFirstName(ids, name);
        assertThat(persons).containsOnly(dave);

        ids = of(List.of(boyd.getId(), dave.getId(), carter.getId()));
        name = of(dave.getFirstName());
        // when in a combined query, "findById" part can receive multiple ids wrapped in QueryParam
        persons = repository.findByIdAndFirstName(ids, name);
        assertThat(persons).containsOnly(dave);

        ids = of(List.of(leroi.getId(), leroi2.getId(), carter.getId()));
        QueryParam firstName = of(leroi.getFirstName());
        QueryParam age = of(leroi2.getAge());
        List<Person> persons2 = repository.findByIdAndFirstNameAndAge(ids, firstName, age);
        assertThat(persons2).containsOnly(leroi2);

        // "findByIdOr..." will return empty list because primary keys are used to firstly narrow down the results.parts
        // In a combined id query "OR" can be put only between the non-id fields like shown below,
        // and the results are limited by the given ids
        ids = of(List.of(leroi.getId(), leroi2.getId(), carter.getId()));
        firstName = of(leroi.getFirstName());
        age = of(leroi2.getAge());
        List<Person> persons3 = repository.findByIdAndFirstNameOrAge(ids, firstName, age);
        assertThat(persons3).containsOnly(leroi, leroi2);

        ids = of(List.of(leroi.getId(), leroi2.getId(), carter.getId()));
        firstName = of(leroi.getFirstName());
        age = of(stefan.getAge());
        List<Person> persons4 = repository.findByIdAndFirstNameOrAge(ids, firstName, age);
        assertThat(persons4).containsOnly(leroi, leroi2);
    }

    @Test
    void findById_dynamicProjection() {
        List<PersonSomeFields> result = repository.findById(dave.getId(), PersonSomeFields.class);
        assertThat(result).containsOnly(dave.toPersonSomeFields());
    }

    @Test
    void findById_AND_simpleProperty_dynamicProjection() {
        QueryParam ids = of(List.of(boyd.getId(), dave.getId(), carter.getId()));
        QueryParam lastName = of(carter.getLastName());
        List<PersonSomeFields> result = repository.findByIdAndLastName(ids, lastName, PersonSomeFields.class);
        assertThat(result).containsOnly(carter.toPersonSomeFields());
    }

    @Test
    void findById_AND_simpleProperty_DynamicProjection_EmptyResult() {
        QueryParam ids = of(List.of(carter.getId(), boyd.getId()));
        QueryParam lastName = of(dave.getLastName());
        List<PersonSomeFields> result = repository.findByIdAndLastName(ids, lastName, PersonSomeFields.class);
        assertThat(result).isEmpty();
    }

    @Test
    void findBySimpleProperty_AND_id_dynamicProjection() {
        QueryParam id = of(dave.getId());
        QueryParam lastName = of(dave.getLastName());
        List<PersonSomeFields> result = repository.findByLastNameAndId(lastName, id, PersonSomeFields.class);
        assertThat(result).containsOnly(dave.toPersonSomeFields());
    }

    @Test
    void findBySimpleProperty_AND_simpleProperty_DynamicProjection() {
        additionalAerospikeTestOperations.createIndex(Person.class, "firstNameIdx", "firstName",
            IndexType.STRING);
        additionalAerospikeTestOperations.createIndex(Person.class, "lastNameIdx", "lastName",
            IndexType.STRING);
        QueryParam firstName = of(carter.getFirstName());
        QueryParam lastName = of(carter.getLastName());
//        List<PersonSomeFields> result = repository.findByFirstNameAndLastName(firstName, lastName,
//            PersonSomeFields.class);
        List<Person> result = repository.findByFirstNameAndLastName(firstName, lastName);
//        assertThat(result).containsOnly(carter.toPersonSomeFields());
        assertThat(result).containsOnly(carter);
    }

    @Test
    void findAllByIds() {
        Iterable<Person> result = repository.findAllById(List.of(dave.getId(), carter.getId()));
        assertThat(result).containsExactlyInAnyOrder(dave, carter);
    }

    @Test
    void findAllByIds_AND_simpleProperty() {
        QueryParam ids1 = of(List.of(dave.getId(), boyd.getId()));
        QueryParam name1 = of(dave.getFirstName());
        List<Person> persons1 = repository.findAllByIdAndFirstName(ids1, name1);
        assertThat(persons1).contains(dave);

        QueryParam ids2 = of(List.of(dave.getId(), boyd.getId()));
        QueryParam name2 = of(carter.getFirstName());
        List<Person> persons2 = repository.findAllByIdAndFirstName(ids2, name2);
        assertThat(persons2).isEmpty();
    }

    @Test
    void findByNestedSimplePropertyEquals() {
        String zipCode = "C0124";
        assertThat(carter.getAddress().getZipCode()).isEqualTo(zipCode);
        assertThat(repository.findByAddressZipCode(zipCode)).containsExactly(carter);

        zipCode = "C012345";
        Address address = new Address("Foo Street 1", 1, zipCode, "Bar");
        dave.setAddress(address);
        repository.save(dave);

        carter.setFriend(dave);
        repository.save(carter);

        List<Person> result = repository.findByFriendAddressZipCode(zipCode);
        assertThat(result).containsExactly(carter);

        // An alternative to "findByFriendAddressZipCode" is using a custom query
        Qualifier nestedZipCodeEq = Qualifier.builder()
            // find records having a map with a key that equals a value
            // POJOs are saved as Maps
            .setFilterOperation(FilterOperation.MAP_VAL_EQ_BY_KEY) // POJOs are saved as Maps
            .setPath("friend.address.zipCode") // path includes bin name, context and the required map key
            .setValue(zipCode) // value of the nested key
            .build();

        Iterable<Person> result2 = repository.findUsingQuery(new Query(nestedZipCodeEq));
        assertThat(result).isEqualTo(result2);
        TestUtils.setFriendsToNull(repository, carter);
    }

    @Test
    void findByNestedSimplePropertyEquals_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByFriendAddressZipCode())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Address.zipCode EQ: invalid number of arguments, expecting one");

        assertThatThrownBy(() -> negativeTestsRepository.findByFriendAddressZipCodeEquals())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Address.zipCode EQ: invalid number of arguments, expecting one");
    }

    private void setSequenceOfFriends() {
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
    }

    // find by deeply nested String POJO field
    @Test
    void findByDeeplyNestedSimplePropertyEquals_PojoField_String_10_levels() {
        String zipCode = "C0123";
        Address address = new Address("Foo Street 1", 1, zipCode, "Bar");
        alicia.setAddress(address);
        repository.save(alicia);

        setSequenceOfFriends();

        List<Person> result =
            repository.findByFriendFriendFriendFriendFriendFriendFriendFriendBestFriendAddressZipCode(zipCode);
        assertThat(result).containsExactly(douglas);

        // An alternative way to perform the same using a custom query
        Qualifier nestedZipCodeEq = Qualifier.builder()
            // find records having a map with a key that equals a value
            // POJOs are saved as Maps
            .setFilterOperation(FilterOperation.MAP_VAL_EQ_BY_KEY) // POJOs are saved as Maps
            // path includes bin name, context and the required map key
            .setPath("friend.friend.friend.friend.friend.friend.friend.friend.bestFriend.address.zipCode")
            .setValue(zipCode) // value of the nested key
            .build();

        Iterable<Person> result2 = repository.findUsingQuery(new Query(nestedZipCodeEq));
        assertThat(result).isEqualTo(result2);

        // cleanup
        TestUtils.setFriendsToNull(repository, allPersons.toArray(Person[]::new));
        alicia.setAddress(null);
        repository.save(alicia);
    }

    // find by deeply nested Integer POJO field
    @Test
    void findByDeeplyNestedSimplePropertyEquals_PojoField_Integer_10_levels() {
        int apartment = 10;
        Address address = new Address("Foo Street 1", apartment, "C0123", "Bar");
        alicia.setAddress(address);
        repository.save(alicia);

        setSequenceOfFriends();

        List<Person> result =
            repository.findByFriendFriendFriendFriendFriendFriendFriendFriendBestFriendAddressApartment(apartment);
        assertThat(result).containsExactly(douglas);

        // An alternative way to perform the same using a custom query
        Qualifier nestedApartmentEq = Qualifier.builder()
            // find records having a map with a key that equals a value
            // POJOs are saved as Maps
            .setFilterOperation(FilterOperation.MAP_VAL_EQ_BY_KEY) // POJOs are saved as Maps
            .setPath("friend.friend.friend.friend.friend.friend.friend.friend.bestFriend.address.apartment") // path
            .setValue(apartment) // value of the nested key
            .build();

        Iterable<Person> result2 = repository.findUsingQuery(new Query(nestedApartmentEq));
        assertThat(result).isEqualTo(result2);

        // cleanup
        TestUtils.setFriendsToNull(repository, allPersons.toArray(Person[]::new));
        alicia.setAddress(null);
        repository.save(alicia);
    }

    // find by deeply nested POJO
    @Test
    void findByDeeplyNestedSimplePropertyEquals_Pojo_9_levels() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            Address address = new Address("Foo Street 1", 1, "C0123", "Bar");
            alicia.setAddress(address);
            repository.save(alicia);

            setSequenceOfFriends();

            List<Person> result =
                repository.findByFriendFriendFriendFriendFriendFriendFriendFriendBestFriendAddress(address);
            assertThat(result).containsExactly(douglas);

            // An alternative way to perform the same using a custom query
            Qualifier nestedAddressEq = Qualifier.builder()
                // find records having a map with a key that equals a value
                // POJOs are saved as Maps
                .setFilterOperation(FilterOperation.MAP_VAL_EQ_BY_KEY) // POJOs are saved as Maps
                .setPath("friend.friend.friend.friend.friend.friend.friend.friend.bestFriend.address") // path
                .setValue(pojoToMap(address)) // value of the nested key
                .build();

            Iterable<Person> result2 = repository.findUsingQuery(new Query(nestedAddressEq));
            assertThat(result).isEqualTo(result2);

            // cleanup
            TestUtils.setFriendsToNull(repository, allPersons.toArray(Person[]::new));
            alicia.setAddress(null);
            repository.save(alicia);
        }
    }

    @Test
    void findByCollectionEquals() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            List<String> listToCompareWith = List.of("str0", "str1", "str2");
            Assertions.assertThat(dave.getStrings()).isEqualTo(listToCompareWith);

            List<Person> persons = repository.findByStringsEquals(listToCompareWith);
            assertThat(persons).contains(dave);

            // another way to call the method
            List<Person> persons2 = repository.findByStrings(listToCompareWith);
            assertThat(persons2).contains(dave);

            List<Person> persons3 = repository.findByIntArray(new int[]{0, 1, 2, 3, 4, 5});
            assertThat(persons3).containsOnly(matias);

            List<Person> persons4 = repository.findByByteArray(new byte[]{1, 0, 1, 1, 0, 0, 0, 1});
            assertThat(persons4).containsOnly(stefan);
        }
    }

    @Test
    void findByCollectionEquals_QueryAnnotation_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByStringsEquals(List.of("string1", "string2")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Element at index 0 is expected to be a String, number or boolean");
    }

    @Test
    void findByCollectionEquals_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByStrings())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.strings EQ: invalid number of arguments, expecting one");

        assertThatThrownBy(() -> negativeTestsRepository.findByStringsEquals("string1", "string2"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.strings EQ: invalid number of arguments, expecting one");

        assertThatThrownBy(() -> negativeTestsRepository.findByStrings(List.of("test"), List.of("test2")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.strings EQ: invalid number of arguments, expecting one");
    }

    @Test
    void findByNestedCollectionEquals() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            var ints = List.of(1, 2, 3, 4);
            dave.setInts(ints);
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendInts(ints);

            assertThat(result).contains(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    void findByMapEquals() {
        if (serverVersionSupport.isFindByCDTSupported()) {
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
    void findByNestedMapEquals() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            var intMap = Map.of("1", 2, "3", 4);
            dave.setIntMap(intMap);
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendIntMap(intMap);

            assertThat(result).contains(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    void findByMapEquals_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByStringMapEquals("map1"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.stringMap EQ: invalid argument type, expecting Map");

        assertThatThrownBy(() -> negativeTestsRepository.findByStringMap(100))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.stringMap EQ: invalid argument type, expecting Map");

        assertThatThrownBy(() -> negativeTestsRepository.findByStringMapEquals(Map.of("key", "value"), Map.of("key",
            "value")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.stringMap EQ: invalid number of arguments, expecting one");
    }

    @Test
    void findByPOJOEquals() {
        if (serverVersionSupport.isFindByCDTSupported()) {
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
    void findByNestedPOJOEquals() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            Address address = new Address("Foo Street 1", 1, "C0123", "Bar");
            dave.setAddress(address);
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendAddress(address);

            assertThat(result).contains(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    void findByNestedPojoEquals_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByFriendAddress())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.address EQ: invalid number of arguments, expecting one POJO");

        assertThatThrownBy(() -> negativeTestsRepository.findByFriendAddressEquals())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.address EQ: invalid number of arguments, expecting one POJO");

        assertThatThrownBy(() -> negativeTestsRepository.findByFriendAddress(100))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.address EQ: Type mismatch, expecting Address");
    }
}
