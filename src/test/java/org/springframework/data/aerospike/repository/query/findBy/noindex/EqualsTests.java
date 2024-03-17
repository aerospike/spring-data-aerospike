package org.springframework.data.aerospike.repository.query.findBy.noindex;

import com.aerospike.client.Value;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.utility.TestUtils;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the "Equals" repository query. Keywords: Is, Equals (or no keyword).
 */
public class EqualsTests extends PersonRepositoryQueryTests {

    @Test
    void findByBooleanIntSimplePropertyEquals() {
        boolean initialValue = Value.UseBoolBin;
        Value.UseBoolBin = false; // save boolean as int
        Person intBoolBinPerson = Person.builder()
            .id(BaseIntegrationTests.nextId())
            .isActive(true)
            .firstName("Test")
            .build();
        repository.save(intBoolBinPerson);

        List<Person> persons = repository.findByIsActive(true);
        assertThat(persons).contains(intBoolBinPerson);

        Value.UseBoolBin = initialValue; // set back to the default value
        repository.delete(intBoolBinPerson);
    }

    @Test
    void findByBooleanIntSimplePropertyEquals_NegativeTest() {
        boolean initialValue = Value.UseBoolBin;
        Value.UseBoolBin = false; // save boolean as int

        assertThatThrownBy(() -> negativeTestsRepository.findByIsActive())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.isActive EQ: invalid number of arguments, expecting one");

        assertThatThrownBy(() -> negativeTestsRepository.findByIsActive(true, false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.isActive EQ: invalid number of arguments, expecting one");

        Value.UseBoolBin = initialValue; // set back to the default value
    }

    @Test
    void findByBooleanSimplePropertyEquals() {
        boolean initialValue = Value.UseBoolBin;
        Value.UseBoolBin = true; // save boolean as bool, available in Server 5.6+
        Person intBoolBinPerson = Person.builder().id(BaseIntegrationTests.nextId()).isActive(true).firstName("Test")
            .build();
        repository.save(intBoolBinPerson);

        List<Person> persons = repository.findByIsActive(true);
        assertThat(persons).contains(intBoolBinPerson);

        Value.UseBoolBin = initialValue; // set back to the default value
        repository.delete(intBoolBinPerson);
    }

    @Test
    void findByBooleanSimplePropertyEquals_NegativeTest() {
        boolean initialValue = Value.UseBoolBin;
        Value.UseBoolBin = true; // save boolean as bool, available in Server 5.6+

        assertThatThrownBy(() -> negativeTestsRepository.findByIsActive())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.isActive EQ: invalid number of arguments, expecting one");

        assertThatThrownBy(() -> negativeTestsRepository.findByIsActive(true, false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.isActive EQ: invalid number of arguments, expecting one");

        Value.UseBoolBin = initialValue; // set back to the default value
    }

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
    void findBySimplePropertyEquals_String_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByFirstName(100))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.firstName EQ: Type mismatch, expecting String");

        assertThatThrownBy(() -> negativeTestsRepository.findByLastName("Beauford", "Boford"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.lastName EQ: invalid number of arguments, expecting one");
    }

    @Test
    void findByNestedSimplePropertyEquals() {
        String zipCode = "C012345";
        Address address = new Address("Foo Street 1", 1, zipCode, "Bar");
        dave.setAddress(address);
        repository.save(dave);

        carter.setFriend(dave);
        repository.save(carter);

        List<Person> result = repository.findByFriendAddressZipCode(zipCode);

        assertThat(result).containsExactly(carter);

        TestUtils.setFriendsToNull(repository, carter);
    }

    @Test
    void findByNestedSimplePropertyEqualsNegativeTest() {
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
        }
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
    void findByNestedPojoEqualsNegativeTest() {
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
