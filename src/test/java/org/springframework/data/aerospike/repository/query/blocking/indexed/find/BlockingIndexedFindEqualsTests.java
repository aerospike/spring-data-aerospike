package org.springframework.data.aerospike.repository.query.blocking.indexed.find;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.Filter;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.annotation.Extensive;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.query.model.IndexKey;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.repository.query.blocking.indexed.IndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.util.TestUtils;

import java.util.ArrayList;
import java.util.List;

import static com.aerospike.client.query.IndexCollectionType.MAPVALUES;
import static com.aerospike.client.query.IndexType.NUMERIC;
import static com.aerospike.client.query.IndexType.STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.query.QueryParam.of;

/**
 * Tests for the "Equals" repository query. Keywords: Is, Equals (or no keyword).
 */
@Extensive
public class BlockingIndexedFindEqualsTests extends IndexedPersonRepositoryQueryTests {

    @Override
    protected List<Index> newIndexes() {
        List<Index> newIndexes = new ArrayList<>();
        String setName = template.getSetName(IndexedPerson.class);
        String postfix = "find_equals";
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_first_name_" + postfix)
                .bin("firstName")
                .indexType(STRING)
                .build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_last_name_" + postfix)
                .bin("lastName")
                .indexType(STRING)
                .build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_age_" + postfix)
                .bin("age")
                .indexType(NUMERIC)
                .build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_isActive_" + postfix)
                .bin("isActive")
                .indexType(STRING)
                .build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_address_values_num_" + postfix)
                .bin("address")
                .indexType(NUMERIC)
                .indexCollectionType(MAPVALUES)
                .build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_address_values_" + postfix)
                .bin("address")
                .indexType(STRING)
                .indexCollectionType(MAPVALUES)
                .build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_friend_address_values_" + postfix)
                .bin("friend")
                .indexType(STRING)
                .indexCollectionType(MAPVALUES)
                .ctx(new CTX[]{CTX.mapKey(Value.get("address"))})
                .build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_friend_bestFriend_addr_val_" + postfix)
                .bin("friend")
                .indexType(STRING)
                .indexCollectionType(MAPVALUES)
                .ctx(new CTX[]{CTX.mapKey(Value.get("bestFriend")), CTX.mapKey(Value.get("address"))})
                .build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_friend_bestFr_addr_val_num_" + postfix)
                .bin("friend")
                .indexType(NUMERIC)
                .indexCollectionType(MAPVALUES)
                .ctx(new CTX[]{CTX.mapKey(Value.get("bestFriend")), CTX.mapKey(Value.get("address"))})
                .build());
        return newIndexes;
    }

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
        QueryParam paramFalse = of(false);
        QueryParam paramTricia = of("Tricia");

        assertQueryHasSecIndexFilter("findByIsActiveAndFirstName", IndexedPerson.class, paramFalse, paramTricia);
        List<IndexedPerson> result = repository.findByIsActiveAndFirstName(paramFalse, paramTricia);

        assertThat(result).containsOnly(tricia);
    }

    @Test
    @AssertBinsAreIndexed(binNames = {"firstName", "age"}, entityClass = IndexedPerson.class)
    public void findByTwoSimplePropertiesEqual_StringAndInteger() {
        QueryParam firstName = of("Billy");
        QueryParam age = of(25);
        assertQueryHasSecIndexFilter("findByFirstNameAndAge", IndexedPerson.class, firstName, age);
        List<IndexedPerson> result = repository.findByFirstNameAndAge(firstName, age);
        assertThat(result).containsOnly(billy);

        firstName = of("Peter");
        age = of(41);
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
                .setValue(zipCode) // value of the nested key
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
                .setValue(zipCode) // value of the nested key
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
                .setValue(apartment) // value of the nested key
                .build();

        Iterable<IndexedPerson> result2 = repository.findUsingQuery(new Query(nestedApartmentEq));
        assertThat(result).isEqualTo(result2);
        TestUtils.setFriendsToNull(repository, jane, peter);
    }

    @Test
    void findBySimpleProperty_AND() {
        Index firstNameIdx = indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "firstName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on firstName"));
        Index ageIdx = indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "age", NUMERIC, null)
        ).orElseThrow(() -> new RuntimeException("No index on age"));
        String binNameChosenForFilter = getBinNameForFilter(firstNameIdx, ageIdx);

        QueryParam firstName = of(john.getFirstName());
        QueryParam age = of(john.getAge());
        String queryName = "findByFirstNameAndAge";
        Filter filter = getQuerySecIndexFilter(queryName, IndexedPerson.class, firstName, age);
        assertThat(filter).isNotNull();
        assertThat(filter.getName()).isEqualTo(binNameChosenForFilter);

        Expression expression = getQueryExpression(queryName, IndexedPerson.class, firstName, age);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.eq(Exp.bin("age", Exp.Type.INT), Exp.val((Integer) age.arguments()[0]))
                )
        );

        List<IndexedPerson> persons = repository.findByFirstNameAndAge(firstName, age);
        assertThat(persons).containsOnly(john);
    }

    @Test
    void findBySimpleProperty_OR() {
        Index firstNameIdx = indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "firstName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on firstName"));
        Index ageIdx = indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "age", NUMERIC, null)
        ).orElseThrow(() -> new RuntimeException("No index on age"));

        QueryParam firstName = of(john.getFirstName());
        QueryParam age = of(peter.getAge());
        String queryName = "findByFirstNameOrAge";
        Filter filter = getQuerySecIndexFilter(queryName, IndexedPerson.class, firstName, age);
        assertThat(filter).isNull();

        Expression expression = getQueryExpression(queryName, IndexedPerson.class, firstName, age);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.or(
                                Exp.eq(Exp.bin("firstName", Exp.Type.STRING), Exp.val((String) firstName.arguments()[0])),
                                Exp.eq(Exp.bin("age", Exp.Type.INT), Exp.val((Integer) age.arguments()[0]))
                        )
                )
        );

        List<IndexedPerson> persons = repository.findByFirstNameOrAge(firstName, age);
        assertThat(persons).containsOnly(john, peter);
    }

    @Test
    void findBySimpleProperty_AND_AND() {
        Index firstNameIdx = indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "firstName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on firstName"));
        Index ageIdx = indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "age", NUMERIC, null)
        ).orElseThrow(() -> new RuntimeException("No index on age"));
        Index lastNameIdx = indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "lastName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on lastName"));
        // This qualifier gets processed first because the tree looks like AND(AND(age, firstName), lastName)
        String binNameChosenForFilter = "lastName";

        QueryParam firstName = of(john.getFirstName());
        QueryParam age = of(john.getAge());
        QueryParam lastName = of(john.getLastName());
        String queryName = "findByFirstNameAndAgeAndLastName";
        Filter filter = getQuerySecIndexFilter(queryName, IndexedPerson.class, firstName, age, lastName);
        assertThat(filter).isNotNull();
        assertThat(filter.getName()).isEqualTo(binNameChosenForFilter);

        Expression expression = getQueryExpression(queryName, IndexedPerson.class, firstName, age, lastName);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.and(
                                Exp.eq(Exp.bin("firstName", Exp.Type.STRING), Exp.val((String) firstName.arguments()[0])),
                                Exp.eq(Exp.bin("age", Exp.Type.INT), Exp.val((Integer) age.arguments()[0]))
                        )
                )
        );

        List<IndexedPerson> persons = repository.findByFirstNameAndAgeAndLastName(firstName, age, lastName);
        assertThat(persons).containsOnly(john);
    }

    @Test
    void findBySimpleProperty_AND_OR() {
        Index firstNameIdx = indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "firstName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on firstName"));
        Index ageIdx = indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "age", NUMERIC, null)
        ).orElseThrow(() -> new RuntimeException("No index on age"));
        Index lastNameIdx = indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "lastName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on lastName"));

        QueryParam firstName = of(john.getFirstName());
        QueryParam age = of(john.getAge());
        QueryParam lastName = of(peter.getLastName());
        String queryName = "findByFirstNameAndAgeOrLastName";
        Filter filter = getQuerySecIndexFilter(queryName, IndexedPerson.class, firstName, age, lastName);
        assertThat(filter).isNull();

        Expression expression = getQueryExpression(queryName, IndexedPerson.class, firstName, age, lastName);
        // The query is divided by OrParts by Spring Data Commons,
        // with OR combination being the upper level: OR(AND(firstName, age), lastName)
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.or(
                                Exp.and(
                                        Exp.eq(Exp.bin("firstName", Exp.Type.STRING), Exp.val((String) firstName.arguments()[0])),
                                        Exp.eq(Exp.bin("age", Exp.Type.INT), Exp.val((Integer) age.arguments()[0]))
                                ),
                                Exp.eq(Exp.bin("lastName", Exp.Type.STRING), Exp.val((String) lastName.arguments()[0]))
                        )
                )
        );

        List<IndexedPerson> persons2 = repository.findByFirstNameAndAgeOrLastName(firstName, age, lastName);
        assertThat(persons2).containsExactlyInAnyOrder(john, peter);
    }

    @Test
    void findBySimpleProperty_OR_AND() {
        indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "firstName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on firstName"));
        indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "age", NUMERIC, null)
        ).orElseThrow(() -> new RuntimeException("No index on age"));
        indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "lastName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on lastName"));

        QueryParam firstName = of(john.getFirstName());
        QueryParam age = of(john.getAge());
        QueryParam lastName = of(peter.getLastName());
        String queryName = "findByFirstNameOrAgeAndLastName";
        Filter filter = getQuerySecIndexFilter(queryName, IndexedPerson.class, firstName, age, lastName);
        assertThat(filter).isNull();

        Expression expression = getQueryExpression(queryName, IndexedPerson.class, firstName, age, lastName);
        // The query is divided by OrParts by Spring Data Commons,
        // with OR combination being the upper level: OR(firstName, AND(age, lastName))
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.or(
                                Exp.eq(Exp.bin("firstName", Exp.Type.STRING), Exp.val((String) firstName.arguments()[0])),
                                Exp.and(
                                        Exp.eq(Exp.bin("age", Exp.Type.INT), Exp.val((Integer) age.arguments()[0])),
                                        Exp.eq(Exp.bin("lastName", Exp.Type.STRING), Exp.val((String) lastName.arguments()[0]))
                                )
                        )
                )
        );

        List<IndexedPerson> persons2 = repository.findByFirstNameOrAgeAndLastName(firstName, age, lastName);
        assertThat(persons2).containsExactlyInAnyOrder(john);
    }

    @Test
    void findBySimpleProperty_OR_OR() {
        indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "firstName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on firstName"));
        indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "age", NUMERIC, null)
        ).orElseThrow(() -> new RuntimeException("No index on age"));
        indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "lastName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on lastName"));

        QueryParam firstName = of(john.getFirstName());
        QueryParam age = of(tricia.getAge());
        QueryParam lastName = of(peter.getLastName());
        String queryName = "findByFirstNameOrAgeOrLastName";
        Filter filter = getQuerySecIndexFilter(queryName, IndexedPerson.class, firstName, age, lastName);
        assertThat(filter).isNull();

        Expression expression = getQueryExpression(queryName, IndexedPerson.class, firstName, age, lastName);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.or(
                                Exp.or(
                                        Exp.eq(Exp.bin("firstName", Exp.Type.STRING), Exp.val((String) firstName.arguments()[0])),
                                        Exp.eq(Exp.bin("age", Exp.Type.INT), Exp.val((Integer) age.arguments()[0]))
                                ),
                                Exp.eq(Exp.bin("lastName", Exp.Type.STRING), Exp.val((String) lastName.arguments()[0]))
                        )
                )
        );

        List<IndexedPerson> persons = repository.findByFirstNameOrAgeOrLastName(firstName, age, lastName);
        assertThat(persons).containsExactlyInAnyOrder(john, tricia, peter);
    }
}
