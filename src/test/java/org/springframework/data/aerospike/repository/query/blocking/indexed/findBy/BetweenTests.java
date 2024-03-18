package org.springframework.data.aerospike.repository.query.blocking.indexed.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.query.blocking.indexed.IndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.aerospike.utility.TestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is between" repository query. Keywords: Between, IsBetween.
 */
public class BetweenTests extends IndexedPersonRepositoryQueryTests {

    @Test
    public void findBySimplePropertyBetween_Integer() {
        Iterable<IndexedPerson> it = repository.findByAgeBetween(40, 45);
        assertThat(it).hasSize(2).contains(john, peter);
    }

    @Test
    public void findBySimplePropertyBetween_Integer_OrderBySimpleProperty() {
        Iterable<IndexedPerson> it = repository.findByAgeBetweenOrderByLastName(30, 45);
        assertThat(it).hasSize(3);
    }

    @Test
    public void findBySimplePropertyBetween_Integer_AND_SimplePropertyEquals_String() {
        QueryParam ageBetween = QueryParam.of(40, 45);
        QueryParam lastNames = QueryParam.of("Matthews");
        Iterable<IndexedPerson> it = repository.findByAgeBetweenAndLastName(ageBetween, lastNames);
        assertThat(it).hasSize(0);

        ageBetween = QueryParam.of(20, 26);
        lastNames = QueryParam.of("Smith");
        Iterable<IndexedPerson> result = repository.findByAgeBetweenAndLastName(ageBetween, lastNames);
        assertThat(result).hasSize(1).contains(billy);
    }

    @Test
    public void findBySimplePropertyBetween_Integer_OR_SimplePropertyEquals_String() {
        QueryParam ageBetween = QueryParam.of(40, 45);
        QueryParam lastNames = QueryParam.of("James");
        Iterable<IndexedPerson> it = repository.findByAgeBetweenOrLastName(ageBetween, lastNames);
        assertThat(it).containsExactlyInAnyOrder(john, peter, tricia);

        ageBetween = QueryParam.of(20, 26);
        lastNames = QueryParam.of("Macintosh");
        Iterable<IndexedPerson> result = repository.findByAgeBetweenOrLastName(ageBetween, lastNames);
        assertThat(result).containsExactlyInAnyOrder(billy, peter);
    }

    @Test
    void findByNestedSimplePropertyBetween_Integer_3_levels() {
        assertThat(jane.getAddress().getApartment()).isEqualTo(2);

        tricia.setFriend(jane);
        repository.save(tricia);
        billy.setBestFriend(tricia);
        repository.save(billy);

        List<IndexedPerson> persons = repository.findByBestFriendFriendAddressApartmentBetween(1, 3);
        assertThat(persons).contains(billy);

        TestUtils.setFriendsToNull(repository, tricia, billy);
    }
}
