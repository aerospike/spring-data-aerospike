package org.springframework.data.aerospike.repository.query.blocking.indexed.findBy;

import com.aerospike.client.Value;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.repository.query.blocking.indexed.IndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.aerospike.util.TestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is between" repository query. Keywords: Between, IsBetween.
 */
public class BetweenTests extends IndexedPersonRepositoryQueryTests {

    @Test
    @AssertBinsAreIndexed(binNames = "age", entityClass = IndexedPerson.class)
    public void findBySimplePropertyBetween_Integer() {
        assertQueryHasSecIndexFilter("findByAgeBetween", IndexedPerson.class, 40, 45);
        Iterable<IndexedPerson> it = repository.findByAgeBetween(40, 45);
        assertThat(it).hasSize(2).contains(john, peter);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "age", entityClass = IndexedPerson.class)
    public void findBySimplePropertyBetween_Integer_OrderBySimpleProperty() {
        assertQueryHasSecIndexFilter("findByAgeBetweenOrderByLastName", IndexedPerson.class, 30, 45);
        Iterable<IndexedPerson> it = repository.findByAgeBetweenOrderByLastName(30, 45);
        assertThat(it).hasSize(3);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "age", entityClass = IndexedPerson.class)
    public void findBySimplePropertyBetween_Integer_AND_SimplePropertyEquals_String() {
        QueryParam ageBetween = QueryParam.of(40, 45);
        QueryParam lastName = QueryParam.of("Matthews");
        assertQueryHasSecIndexFilter("findByAgeBetweenAndLastName", IndexedPerson.class, ageBetween, lastName);
        Iterable<IndexedPerson> it = repository.findByAgeBetweenAndLastName(ageBetween, lastName);
        assertThat(it).hasSize(0);

        ageBetween = QueryParam.of(20, 26);
        lastName = QueryParam.of("Smith");
        assertQueryHasSecIndexFilter("findByAgeBetweenAndLastName", IndexedPerson.class, ageBetween, lastName);
        Iterable<IndexedPerson> result = repository.findByAgeBetweenAndLastName(ageBetween, lastName);
        assertThat(result).hasSize(1).contains(billy);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "age", entityClass = IndexedPerson.class)
    public void findBySimplePropertyBetween_Integer_OR_SimplePropertyEquals_String() {
        QueryParam ageBetween = QueryParam.of(40, 45);
        QueryParam lastName = QueryParam.of("James");
        assertThat(queryHasSecIndexFilter("findByAgeBetween", IndexedPerson.class, 40, 45)
            || queryHasSecIndexFilter("findByLastName", IndexedPerson.class, "James")).isTrue();
        Iterable<IndexedPerson> it = repository.findByAgeBetweenOrLastName(ageBetween, lastName);
        assertThat(it).containsExactlyInAnyOrder(john, peter, tricia);

        ageBetween = QueryParam.of(20, 26);
        lastName = QueryParam.of("Macintosh");
        assertThat(queryHasSecIndexFilter("findByAgeBetween", IndexedPerson.class, 20, 26)
            || queryHasSecIndexFilter("findByLastName", IndexedPerson.class, "Macintosh")).isTrue();
        Iterable<IndexedPerson> result = repository.findByAgeBetweenOrLastName(ageBetween, lastName);
        assertThat(result).containsExactlyInAnyOrder(billy, peter);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "bestFriend", entityClass = IndexedPerson.class)
    void findByNestedSimplePropertyBetween_Integer_3_levels() {
        int apartment = 2;
        assertThat(jane.getAddress().getApartment()).isEqualTo(apartment);
        tricia.setFriend(jane);
        repository.save(tricia);
        billy.setBestFriend(tricia);
        repository.save(billy);
        assertThat(billy.getBestFriend().getFriend().getAddress().getApartment()).isEqualTo(apartment);

        assertQueryHasSecIndexFilter("findByBestFriendFriendAddressApartmentBetween", IndexedPerson.class, 1, 3);
        List<IndexedPerson> persons = repository.findByBestFriendFriendAddressApartmentBetween(1, 3);
        assertThat(persons).contains(billy);

        // An alternative way to perform the same using a custom query
        Qualifier nestedApartmentBetween = Qualifier.builder()
            // find records having a map with a key between given values
            // POJOs are saved as Maps
            .setFilterOperation(FilterOperation.MAP_VAL_BETWEEN_BY_KEY) // POJOs are saved as Maps
            .setPath("bestFriend.friend.address.apartment") // path includes bin name, context and the required map key
            .setValue(Value.get(1)) // lower limit for the value of the nested key
            .setSecondValue(Value.get(3)) // lower limit for the value of the nested key
            .build();

        assertQueryHasSecIndexFilter(new Query(nestedApartmentBetween), IndexedPerson.class);
        Iterable<IndexedPerson> persons2 = repository.findUsingQuery(new Query(nestedApartmentBetween));
        assertThat(persons).isEqualTo(persons2);
        TestUtils.setFriendsToNull(repository, tricia, billy);
    }
}
