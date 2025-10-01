package org.springframework.data.aerospike.repository.query.blocking.noindex.find;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.noindex.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.util.TestUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the "Is not equal" repository query. Keywords: Not, IsNot.
 */
public class NotEqualTests extends PersonRepositoryQueryTests {

    @Test
    void findBySimplePropertyNotEqual_String() {
        Stream<Person> result = repository.findByLastNameNot("Moore");
        assertThat(result)
            .doesNotContain(leroi, leroi2)
            .contains(dave, donny, oliver, carter, boyd, stefan, alicia);

        List<Person> result2 = repository.findByFirstNameNot("Leroi");
        assertThat(result2).doesNotContain(leroi, leroi2);

        List<Person> result3 = repository.findByFirstNameNotIgnoreCase("lEroi");
        assertThat(result3).doesNotContain(leroi, leroi2);

        List<Person> result4 = repository.findByFirstNameNot("lEroi");
        assertThat(result4).contains(leroi, leroi2);
    }

    @Test
    void findByNestedSimplePropertyNotEqual() {
        oliver.setFriend(alicia);
        repository.save(oliver);
        dave.setFriend(oliver);
        repository.save(dave);
        carter.setFriend(dave);
        repository.save(carter);
        assertThat(carter.getFriend().getAge()).isEqualTo(42);

        // find all records where friend's age is not 42 and all without friend.age
        List<Person> result = repository.findByFriendAgeIsNot(42);
        assertThat(result).doesNotContain(carter);

        TestUtils.setFriendsToNull(repository, oliver, dave, carter);
    }

    @Test
    void findByNestedSimplePropertyNotEqual_ZipCode() {
        assertThat(carter.getAddress().getZipCode()).isEqualTo("C0124");
        assertThat(dave.getAddress().getZipCode()).isEqualTo("C0123");
        // find all records where address' zipCode is not C0123 or C0125, and all without address.zipCode
        assertThat(repository.findByAddressZipCodeIsNot("C0123"))
            .containsOnly(donny, oliver, alicia, boyd, stefan, leroi, leroi2, matias, douglas, carter);
    }

    @Test
    void findByNestedSimplePropertyNotEqual_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByFriendAddressZipCodeIsNot())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Address.zipCode NOTEQ: invalid number of arguments, expecting one");
    }

    @Test
    void findByCollectionNotEqual() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            List<String> listToCompareWith = List.of("str0", "str1", "str2");
            assertThat(dave.getStrings()).isEqualTo(listToCompareWith);
            assertThat(donny.getStrings()).isNotEmpty();
            assertThat(donny.getStrings()).isNotEqualTo(listToCompareWith);

            List<Person> persons = repository.findByStringsIsNot(listToCompareWith);
            assertThat(persons).contains(donny);
        }
    }

    @Test
    void findByCollection_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByStringsIsNot())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.strings NOTEQ: invalid number of arguments, expecting one");

        assertThatThrownBy(() -> negativeTestsRepository.findByStringsIsNot("string1", "string2"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.strings NOTEQ: invalid number of arguments, expecting one");

        assertThatThrownBy(() -> negativeTestsRepository.findByStringsIsNot(List.of("string1"), List.of("string2")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.strings NOTEQ: invalid number of arguments, expecting one");
    }

    @Test
    void findByNestedCollectionNotEqual() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            dave.setInts(List.of(1, 2, 3, 4));
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendIntsIsNot(List.of(0, 1, 2, 3, 4, 5, 6, 7));

            assertThat(result).contains(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    void findByMapNotEqual() {
        if (serverVersionSupport.isFindByCDTSupported()) {
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
    void findByMapNotEqual_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByStringMapIsNot("map1"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.stringMap NOTEQ: invalid argument type, expecting Map");

        assertThatThrownBy(() -> negativeTestsRepository.findByStringMapIsNot(Map.of("key", "value"), Map.of("key",
            "value")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.stringMap NOTEQ: invalid number of arguments, expecting one");
    }

    @Test
    void findByNestedMapNotEqual() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            dave.setIntMap(Map.of("1", 2, "3", 4));
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendIntMapIsNot(Map.of("0", 1, "2", 3, "4", 5, "6", 7));

            assertThat(result).contains(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    void findByPOJONotEqual() {
        if (serverVersionSupport.isFindByCDTSupported()) {
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
    void findByPOJONotEqual_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByAddressIsNot())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.address NOTEQ: invalid number of arguments, expecting one POJO");

        assertThatThrownBy(() -> negativeTestsRepository.findByAddressIsNot(100))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.address NOTEQ: Type mismatch, expecting Address");
    }

    @Test
    void findByNestedPojoNotEqual() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            Address address = new Address("Foo Street 1", 100, "C0123", "Bar");
            assertThat(dave.getAddress()).isNotEqualTo(address);

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendAddressIsNot(address);

            assertThat(result).contains(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    void findByNestedPOJONotEqual_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByFriendAddressIsNot())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.address NOTEQ: invalid number of arguments, expecting one POJO");

        assertThatThrownBy(() -> negativeTestsRepository.findByFriendAddressIsNot(100))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.address NOTEQ: Type mismatch, expecting Address");
    }
}
