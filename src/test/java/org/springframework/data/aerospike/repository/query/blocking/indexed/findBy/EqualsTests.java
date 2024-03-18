package org.springframework.data.aerospike.repository.query.blocking.indexed.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.query.blocking.indexed.IndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.aerospike.utility.TestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Equals" repository query. Keywords: Is, Equals (or no keyword).
 */
public class EqualsTests extends IndexedPersonRepositoryQueryTests {

    @Test
    public void findBySimplePropertyEquals_String() {
        List<IndexedPerson> result = repository.findByLastName("Gillaham");
        assertThat(result).containsOnly(jane);

        List<IndexedPerson> result2 = repository.findByFirstName("Tricia");
        assertThat(result2).containsOnly(tricia);
    }

    @Test
    public void findByTwoSimplePropertiesEqual_StringAndBoolean() {
        assertThat(tricia.isActive()).isFalse();
        QueryParam isActive = QueryParam.of(false);
        QueryParam firstNames = QueryParam.of("Tricia");
        List<IndexedPerson> result = repository.findByIsActiveAndFirstName(isActive, firstNames);

        assertThat(result)
            .hasSize(1)
            .containsOnly(tricia);
    }

    @Test
    public void findByTwoSimplePropertiesEqual_StringAndInteger() {
        QueryParam firstNames = QueryParam.of("Billy");
        QueryParam ages = QueryParam.of(25);
        List<IndexedPerson> result = repository.findByFirstNameAndAge(firstNames, ages);
        assertThat(result).containsOnly(billy);

        firstNames = QueryParam.of("Peter");
        ages = QueryParam.of(41);
        result = repository.findByFirstNameAndAge(firstNames, ages);
        assertThat(result).containsOnly(peter);
    }

    @Test
    void findByNestedSimpleProperty_String() {
        assertThat(john.getAddress().getZipCode()).isEqualTo("C0123");
        List<IndexedPerson> result = repository.findByAddressZipCode("C0123");
        assertThat(result).contains(john);
    }

    @Test
    void findByNestedSimpleProperty_String_2_levels() {
        assertThat(john.getAddress().getZipCode()).isEqualTo("C0123");
        jane.setFriend(john);
        repository.save(jane);

        List<IndexedPerson> result = repository.findByFriendAddressZipCode("C0123");
        assertThat(result).contains(jane);
        TestUtils.setFriendsToNull(repository, jane);
    }

    @Test
    void findByNestedSimpleProperty_String_3_levels() {
        assertThat(john.getAddress().getZipCode()).isEqualTo("C0123");
        jane.setBestFriend(john);
        repository.save(jane);
        peter.setFriend(jane);
        repository.save(peter);

        List<IndexedPerson> result = repository.findByFriendBestFriendAddressZipCode("C0123");
        assertThat(result).contains(peter);
        TestUtils.setFriendsToNull(repository, jane, peter);
    }

    @Test
    void findByNestedSimpleProperty_Integer_3_levels() {
        assertThat(john.getAddress().getApartment()).isEqualTo(1);
        jane.setBestFriend(john);
        repository.save(jane);
        peter.setFriend(jane);
        repository.save(peter);

        List<IndexedPerson> result = repository.findByFriendBestFriendAddressApartment(1);
        assertThat(result).contains(peter);
        TestUtils.setFriendsToNull(repository, jane, peter);
    }
}
