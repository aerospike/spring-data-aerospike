package org.springframework.data.aerospike.repository.query.blocking.indexed.findBy;

import com.aerospike.client.Value;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.query.QueryParam;
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
        assertStmtHasSecIndexFilter("findByLastName", IndexedPerson.class, "Gillaham");
        List<IndexedPerson> result = repository.findByLastName("Gillaham");
        assertThat(result).containsOnly(jane);

        assertStmtHasSecIndexFilter("findByFirstName", IndexedPerson.class, "Tricia");
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

        // Secondary index filter for a boolean value is not supported
        assertThat(stmtHasSecIndexFilter("findByIsActive", IndexedPerson.class, true)).isFalse();
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

        assertStmtHasSecIndexFilter("findByIsActiveAndFirstName", IndexedPerson.class, paramFalse, paramTricia);
        List<IndexedPerson> result = repository.findByIsActiveAndFirstName(paramFalse, paramTricia);

        assertThat(result).containsOnly(tricia);
    }

    @Test
    @AssertBinsAreIndexed(binNames = {"firstName", "age"}, entityClass = IndexedPerson.class)
    public void findByTwoSimplePropertiesEqual_StringAndInteger() {
        QueryParam firstName = QueryParam.of("Billy");
        QueryParam age = QueryParam.of(25);
        assertStmtHasSecIndexFilter("findByFirstNameAndAge", IndexedPerson.class, firstName, age);
        List<IndexedPerson> result = repository.findByFirstNameAndAge(firstName, age);
        assertThat(result).containsOnly(billy);

        firstName = QueryParam.of("Peter");
        age = QueryParam.of(41);
        assertStmtHasSecIndexFilter("findByFirstNameAndAge", IndexedPerson.class, firstName, age);
        result = repository.findByFirstNameAndAge(firstName, age);
        assertThat(result).containsOnly(peter);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "address", entityClass = IndexedPerson.class)
    void findByNestedSimpleProperty_String() {
        String zipCode = "C0123";
        assertThat(john.getAddress().getZipCode()).isEqualTo(zipCode);
        assertStmtHasSecIndexFilter("findByAddressZipCode", IndexedPerson.class, zipCode);
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

        // Currently nested queries don't have secondary index filter
//        assertStmtHasSecIndexFilter("findByFriendAddressZipCode", IndexedPerson.class, zipCode);
        List<IndexedPerson> result = repository.findByFriendAddressZipCode(zipCode);
        assertThat(result).contains(jane);
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

        // Currently deeply nested queries don't have secondary index filter
//        assertStmtHasSecIndexFilter("findByFriendBestFriendAddressZipCode", IndexedPerson.class, zipCode);
        List<IndexedPerson> result = repository.findByFriendBestFriendAddressZipCode(zipCode);
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

        // Currently deeply nested queries don't have secondary index filter
//        assertStmtHasSecIndexFilter("findByFriendBestFriendAddressApartment", IndexedPerson.class, apartment);
        List<IndexedPerson> result = repository.findByFriendBestFriendAddressApartment(apartment);
        assertThat(result).contains(peter);
        TestUtils.setFriendsToNull(repository, jane, peter);
    }
}
