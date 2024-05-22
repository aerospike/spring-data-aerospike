package org.springframework.data.aerospike.repository.query.blocking.indexed.findBy;

import com.aerospike.client.Value;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.repository.query.blocking.indexed.IndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.util.TestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Equals" repository query. Keywords: Is, Equals (or no keyword).
 */
public class EqualsTests extends IndexedPersonRepositoryQueryTests {

    @Test
    @AssertBinsAreIndexed(binNames = "lastName", entityClass = IndexedPerson.class)
    public void findBySimplePropertyEquals_String() {
        assertQueryHasSecIndexFilter("findByLastName", IndexedPerson.class, "Gillaham");
        List<IndexedPerson> result = repository.findByLastName("Gillaham");
        assertThat(result).containsOnly(jane);

        assertQueryHasSecIndexFilter("findByFirstName", IndexedPerson.class, "Tricia");
        assertBinIsIndexed("firstName", IndexedPerson.class);
        List<IndexedPerson> result2 = repository.findByFirstName("Tricia");
        assertThat(result2).containsOnly(tricia);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "isActive", entityClass = IndexedPerson.class)
    void findBySimplePropertyEquals_Boolean_NoSecondaryIndexFilter() {
        boolean initialValue = Value.UseBoolBin;
        Value.UseBoolBin = true; // save boolean as bool, available since Server 5.6+

        IndexedPerson boolBinPerson = IndexedPerson.from(
            Person.builder()
                .id(BaseIntegrationTests.nextId())
                .isActive(true)
                .firstName("Test")
                .build()
        );
        repository.save(boolBinPerson);

        assertThat(queryHasSecIndexFilter("findByIsActive", IndexedPerson.class, true)).isFalse();
        assertThat(repository.findByIsActive(true)).contains(boolBinPerson);

        Value.UseBoolBin = initialValue; // set back to the default value
        repository.delete(boolBinPerson);
    }

    @Test
    @AssertBinsAreIndexed(binNames = {"isActive", "firstName"}, entityClass = IndexedPerson.class)
    public void findByTwoSimplePropertiesEqual_BooleanAndString() {
        assertThat(tricia.isActive()).isFalse();
        QueryParam paramFalse = QueryParam.of(false);
        QueryParam paramTricia = QueryParam.of("Tricia");

        assertQueryHasSecIndexFilter("findByIsActiveAndFirstName", IndexedPerson.class, paramFalse, paramTricia);
        List<IndexedPerson> result = repository.findByIsActiveAndFirstName(paramFalse, paramTricia);

        assertThat(result).containsOnly(tricia);
    }

    @Test
    @AssertBinsAreIndexed(binNames = {"firstName", "age"}, entityClass = IndexedPerson.class)
    public void findByTwoSimplePropertiesEqual_StringAndInteger() {
        QueryParam firstName = QueryParam.of("Billy");
        QueryParam age = QueryParam.of(25);
        assertQueryHasSecIndexFilter("findByFirstNameAndAge", IndexedPerson.class, firstName, age);
        List<IndexedPerson> result = repository.findByFirstNameAndAge(firstName, age);
        assertThat(result).containsOnly(billy);

        firstName = QueryParam.of("Peter");
        age = QueryParam.of(41);
        assertQueryHasSecIndexFilter("findByFirstNameAndAge", IndexedPerson.class, firstName, age);
        result = repository.findByFirstNameAndAge(firstName, age);
        assertThat(result).containsOnly(peter);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "address", entityClass = IndexedPerson.class)
    void findByNestedSimpleProperty_String() {
        String zipCode = "C0123";
        assertThat(john.getAddress().getZipCode()).isEqualTo(zipCode);
        assertQueryHasSecIndexFilter("findByAddressZipCode", IndexedPerson.class, zipCode);
        List<IndexedPerson> result = repository.findByAddressZipCode(zipCode);
        assertThat(result).contains(john);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "friend", entityClass = IndexedPerson.class)
    void findByNestedSimpleProperty_String_2_levels() {
        String zipCode = "C0123";
        assertThat(john.getAddress().getZipCode()).isEqualTo(zipCode);
        jane.setFriend(john);
        repository.save(jane);

        assertQueryHasSecIndexFilter("findByFriendAddressZipCode", IndexedPerson.class, zipCode);
        List<IndexedPerson> result = repository.findByFriendAddressZipCode(zipCode);
        assertThat(result).contains(jane);

        // An alternative way to perform the same using a custom query
        Qualifier nestedZipCodeEq = Qualifier.builder()
            // find records having a map with a key that equals a value
            // POJOs are saved as Maps
            .setFilterOperation(FilterOperation.MAP_VAL_EQ_BY_KEY) // POJOs are saved as Maps
            .setPath("friend.address.zipCode") // path
            .setValue(Value.get(zipCode)) // value of the nested key
            .build();

        assertQueryHasSecIndexFilter(new Query(nestedZipCodeEq), IndexedPerson.class);
        Iterable<IndexedPerson> result2 = repository.findUsingQuery(new Query(nestedZipCodeEq));
        assertThat(result).isEqualTo(result2);
        TestUtils.setFriendsToNull(repository, jane);
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

        assertQueryHasSecIndexFilter("findByFriendBestFriendAddressZipCode", IndexedPerson.class, zipCode);
        List<IndexedPerson> result = repository.findByFriendBestFriendAddressZipCode(zipCode);
        assertThat(result).contains(peter);

        // An alternative way to perform the same using a custom query
        Qualifier nestedZipCodeEq = Qualifier.builder()
            .setFilterOperation(FilterOperation.MAP_VAL_EQ_BY_KEY) // POJOs are saved as Maps
            .setPath("friend.bestFriend.address.zipCode") // path
            .setValue(Value.get(zipCode)) // value of the nested key
            .build();

        assertQueryHasSecIndexFilter(new Query(nestedZipCodeEq), IndexedPerson.class);
        Iterable<IndexedPerson> result2 = repository.findUsingQuery(new Query(nestedZipCodeEq));
        assertThat(result).isEqualTo(result2);
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

        assertQueryHasSecIndexFilter("findByFriendBestFriendAddressApartment", IndexedPerson.class, apartment);
        List<IndexedPerson> result = repository.findByFriendBestFriendAddressApartment(apartment);
        assertThat(result).contains(peter);

        // An alternative way to perform the same using a custom query
        Qualifier nestedApartmentEq = Qualifier.builder()
            .setFilterOperation(FilterOperation.MAP_VAL_EQ_BY_KEY) // POJOs are saved as Maps
            .setPath("friend.bestFriend.address.apartment") // path
            .setValue(Value.get(apartment)) // value of the nested key
            .build();

        Iterable<IndexedPerson> result2 = repository.findUsingQuery(new Query(nestedApartmentEq));
        assertThat(result).isEqualTo(result2);
        TestUtils.setFriendsToNull(repository, jane, peter);
    }
}
