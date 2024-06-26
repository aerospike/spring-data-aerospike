package org.springframework.data.aerospike.repository.query.blocking.indexed.findBy;

import com.aerospike.client.Value;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.repository.query.blocking.indexed.IndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.aerospike.util.TestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class CustomQueriesTests extends IndexedPersonRepositoryQueryTests {

    @Test
    @AssertBinsAreIndexed(binNames = "friend", entityClass = IndexedPerson.class)
    void findByNestedSimpleProperty_String_map_in_map() {
        String zipCode = "C0123";
        assertThat(john.getAddress().getZipCode()).isEqualTo(zipCode);
        jane.setFriend(john);
        repository.save(jane);

        Qualifier nestedZipCodeEq = Qualifier.builder()
            // find records having a nested map with a key that equals a value
            // POJOs are saved as Maps
            .setFilterOperation(FilterOperation.MAP_VAL_EQ_BY_KEY) // where address's key 'zipCode' has value "C0123"
            .setPath("friend.address.zipCode") // path includes bin name, context and the required map key
            .setValue(zipCode) // value of the nested key
            .build();

        assertQueryHasSecIndexFilter(new Query(nestedZipCodeEq), IndexedPerson.class);
        Iterable<IndexedPerson> result = repository.findUsingQuery(new Query(nestedZipCodeEq));
        assertThat(result).contains(jane);
        TestUtils.setFriendsToNull(repository, jane);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "friend", entityClass = IndexedPerson.class)
    void findByNestedSimpleProperty_String_map_in_list() {
        String zipCode = "ZipCode";
        john.setAddressesList(List.of(new Address("Street", 100, zipCode, "City")));
        repository.save(john);
        assertThat(john.getAddressesList().get(0).getZipCode()).isEqualTo(zipCode);

        Qualifier nestedZipCodeEq = Qualifier.builder()
            // find records having a nested map with a key that equals a value
            // POJOs are saved as Maps
            .setFilterOperation(FilterOperation.MAP_VAL_EQ_BY_KEY) // where list element 'zipCode' has value "ZipCode"
            .setPath("addressesList.[0].zipCode") // path: bin name, context (list index) and the required map key
            .setValue(Value.get(zipCode)) // value of the nested key
            .build();

        assertQueryHasSecIndexFilter(new Query(nestedZipCodeEq), IndexedPerson.class);
        Iterable<IndexedPerson> resultTest2 = repository.findUsingQuery(new Query(nestedZipCodeEq));
        Assertions.assertThat(resultTest2).contains(john);
        john.setAddressesList(null);
        repository.save(john);
    }


    @Test
    @AssertBinsAreIndexed(binNames = "friend", entityClass = IndexedPerson.class)
    void findByNestedSimpleProperty_String_3_levels() {
        String zipCode = "C0123";
        assertThat(john.getAddress().getZipCode()).isEqualTo(zipCode);
        jane.setBestFriend(john);
        repository.save(jane);
        peter.setFriend(jane);
        repository.save(peter);

        Qualifier nestedZipCodeEq = Qualifier.builder()
            // find records having a nested map with a key that equals a value
            // POJOs are saved as Maps
            .setFilterOperation(FilterOperation.MAP_VAL_EQ_BY_KEY) // where address's key 'zipCode' has value "C0123"
            .setPath("friend.bestFriend.address.zipCode") // path includes bin name, context and the required map key
            .setValue(Value.get(zipCode)) // value of the nested key
            .build();

        assertQueryHasSecIndexFilter(new Query(nestedZipCodeEq), IndexedPerson.class);
        Iterable<IndexedPerson> result = repository.findUsingQuery(new Query(nestedZipCodeEq));
        assertThat(result).contains(peter);
        TestUtils.setFriendsToNull(repository, jane, peter);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "friend", entityClass = IndexedPerson.class)
    void findByNestedSimpleProperty_Integer_3_levels() {
        int apartment = 1;
        assertThat(john.getAddress().getApartment()).isEqualTo(apartment);
        jane.setBestFriend(john);
        repository.save(jane);
        peter.setFriend(jane);
        repository.save(peter);

        Qualifier nestedApartmentEq = Qualifier.builder()
            .setFilterOperation(FilterOperation.MAP_VAL_EQ_BY_KEY) // where address's key 'apartment' has value 1
            .setPath("friend.bestFriend.address.apartment") // path includes bin name, context and the required map key
            .setValue(Value.get(apartment)) // value of the nested key
            .build();

        assertQueryHasSecIndexFilter(new Query(nestedApartmentEq), IndexedPerson.class);
        Iterable<IndexedPerson> result = repository.findUsingQuery(new Query(nestedApartmentEq));
        assertThat(result).contains(peter);
        TestUtils.setFriendsToNull(repository, jane, peter);
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

        // An alternative way to perform the same using a custom query
        Qualifier nestedApartmentBetween = Qualifier.builder()
            // find records having a map with a key between given values
            // POJOs are saved as Maps
            // where address's key 'apartment' has value between 1 and 3
            .setFilterOperation(FilterOperation.MAP_VAL_BETWEEN_BY_KEY)
            .setPath("bestFriend.friend.address.apartment") // path includes bin name, context and the required map key
            .setValue(Value.get(1)) // lower limit for the value of the nested key
            .setSecondValue(Value.get(3)) // lower limit for the value of the nested key
            .build();

        assertQueryHasSecIndexFilter(new Query(nestedApartmentBetween), IndexedPerson.class);
        Iterable<IndexedPerson> persons = repository.findUsingQuery(new Query(nestedApartmentBetween));
        assertThat(persons).contains(billy);
        TestUtils.setFriendsToNull(repository, tricia, billy);
    }
}

