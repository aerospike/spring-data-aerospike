package org.springframework.data.aerospike.repository.query.blocking.indexed.find;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.Filter;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.annotation.Nightly;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.query.model.IndexKey;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.repository.query.blocking.indexed.IndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.util.TestUtils;

import java.util.ArrayList;
import java.util.List;

import static com.aerospike.client.query.IndexCollectionType.MAPVALUES;
import static com.aerospike.client.query.IndexType.NUMERIC;
import static com.aerospike.client.query.IndexType.STRING;
import static org.assertj.core.api.Assertions.assertThat;

@Nightly
public class BlockingIndexedFindCustomQueriesTests extends IndexedPersonRepositoryQueryTests {

    @Override
    protected List<Index> newIndexes() {
        List<Index> newIndexes = new ArrayList<>();
        String setName = template.getSetName(IndexedPerson.class);
        String postfix = "find_custom";
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
                .name("indexed_person_gender_" + postfix)
                .bin("gender")
                .indexType(STRING)
                .build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_addressesList_0_values_" + postfix)
                .bin("addressesList")
                .indexType(STRING)
                .indexCollectionType(MAPVALUES)
                .ctx(new CTX[]{CTX.listIndex(0)})
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
                .name("indexed_person_friend_bestFriend_address_values_" + postfix)
                .bin("friend")
                .indexType(STRING)
                .indexCollectionType(MAPVALUES)
                .ctx(new CTX[]{CTX.mapKey(Value.get("bestFriend")), CTX.mapKey(Value.get("address"))})
                .build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_friend_bestFriend_addr_val_num_" + postfix)
                .bin("friend")
                .indexType(NUMERIC)
                .indexCollectionType(MAPVALUES)
                .ctx(new CTX[]{CTX.mapKey(Value.get("bestFriend")), CTX.mapKey(Value.get("address"))})
                .build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_bestFriend_friend_addr_val_" + postfix)
                .bin("bestFriend")
                .indexType(STRING)
                .indexCollectionType(MAPVALUES)
                .ctx(new CTX[]{CTX.mapKey(Value.get("friend")), CTX.mapKey(Value.get("address"))})
                .build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_bestFriend_friend_addr_val_num_" + postfix)
                .bin("bestFriend")
                .indexType(NUMERIC)
                .indexCollectionType(MAPVALUES)
                .ctx(new CTX[]{CTX.mapKey(Value.get("friend")), CTX.mapKey(Value.get("address"))})
                .build());
        return newIndexes;
    }

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
                .setFilterOperation(FilterOperation.MAP_VAL_EQ_BY_KEY) // POJOs are saved as Maps
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
                .setFilterOperation(FilterOperation.MAP_VAL_EQ_BY_KEY)
                .setPath("addressesList.[0].zipCode") // path: bin name, context (list index) and the required map key
                .setValue(zipCode) // value of the nested key
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
                .setFilterOperation(FilterOperation.MAP_VAL_EQ_BY_KEY) // POJOs are saved as Maps
                .setPath("friend.bestFriend.address.zipCode") // path includes bin name, context and the required map key
                .setValue(zipCode) // value of the nested key
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
                .setFilterOperation(FilterOperation.MAP_VAL_EQ_BY_KEY) // POJOs are saved as Maps
                .setPath("friend.bestFriend.address.apartment") // path includes bin name, context and the required map key
                .setValue(apartment) // value of the nested key
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
                .setFilterOperation(FilterOperation.MAP_VAL_BETWEEN_BY_KEY) // POJOs are saved as Maps
                .setPath("bestFriend.friend.address.apartment") // path includes bin name, context and the required map key
                .setValue(1) // lower limit for the value of the nested key
                .setSecondValue(3) // lower limit for the value of the nested key
                .build();

        assertQueryHasSecIndexFilter(new Query(nestedApartmentBetween), IndexedPerson.class);
        Iterable<IndexedPerson> persons = repository.findUsingQuery(new Query(nestedApartmentBetween));
        assertThat(persons).contains(billy);
        TestUtils.setFriendsToNull(repository, tricia, billy);
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

        // creating an expression "firstName is equal to John"
        Qualifier firstNameEq = Qualifier.builder()
                .setPath("firstName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("John")
                .build();
        // creating an expression "age is equal to 42"
        Qualifier ageEq = Qualifier.builder()
                .setPath("age")
                .setFilterOperation(FilterOperation.EQ)
                .setValue(42)
                .build();

        Query query = new Query(Qualifier.and(firstNameEq, ageEq));
        Filter filter = getQuerySecIndexFilter(query, IndexedPerson.class);
        assertThat(filter).isNotNull();
        assertThat(filter.getName()).isEqualTo(binNameChosenForFilter);

        Expression expression = getQueryExpression(query, IndexedPerson.class);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.eq(Exp.bin("age", Exp.Type.INT), Exp.val((Integer) ageEq.getValue().getObject()))
                )
        );

        // conditions are combined with AND
        Iterable<IndexedPerson> result = repository.findUsingQuery(query);
        assertThat(result).containsOnly(john);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "firstName", entityClass = IndexedPerson.class)
    void findBySimpleProperty_AND_negative() {
        Qualifier firstNameEqJohn = Qualifier.builder()
                .setPath("firstName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("John")
                .build();
        Qualifier firstNameEqPeter = Qualifier.builder()
                .setPath("firstName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("Peter")
                .build();

        Query query = new Query(Qualifier.and(firstNameEqJohn, firstNameEqPeter));
        Filter filter = getQuerySecIndexFilter(query, IndexedPerson.class);
        assertThat(filter).isNotNull();
        assertThat(filter.getName()).isEqualTo("firstName");

        Expression expression = getQueryExpression(query, IndexedPerson.class);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.eq(Exp.bin("firstName", Exp.Type.STRING), Exp.val((String) firstNameEqPeter.getValue().getObject()))
                )
        );

        Iterable<IndexedPerson> result = repository.findUsingQuery(query);
        // First name cannot be simultaneously John and Peter
        assertThat(result).isEmpty();
    }

    @Test
    @AssertBinsAreIndexed(binNames = "firstName", entityClass = IndexedPerson.class)
    void findBySimpleProperty_OR() {
        // creating an expression "firstName is equal to John"
        Qualifier firstNameEqJohn = Qualifier.builder()
                .setPath("firstName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("John")
                .build();
        // creating an expression "firstName is equal to Peter"
        Qualifier firstNameEqPeter = Qualifier.builder()
                .setPath("firstName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("Peter")
                .build();

        Query query = new Query(Qualifier.or(firstNameEqJohn, firstNameEqPeter));
        Filter filter = getQuerySecIndexFilter(query, IndexedPerson.class);
        // No single unifying secondary index Filter
        assertThat(filter).isNull();

        Expression expression = getQueryExpression(query, IndexedPerson.class);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.or(
                                Exp.eq(Exp.bin("firstName", Exp.Type.STRING), Exp.val((String) firstNameEqJohn.getValue().getObject())),
                                Exp.eq(Exp.bin("firstName", Exp.Type.STRING), Exp.val((String) firstNameEqPeter.getValue().getObject()))
                        )
                )
        );

        Iterable<IndexedPerson> result = repository.findUsingQuery(query);
        assertThat(result).containsExactlyInAnyOrder(john, peter);
    }

    @Test
    void findBySimpleProperty_AND_3elements() {
        Index firstNameIdx = indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "firstName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on firstName"));
        Index ageIdx = indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "age", NUMERIC, null)
        ).orElseThrow(() -> new RuntimeException("No index on age"));
        Index lastNameIdx = indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "lastName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on lastName"));
        String binNameChosenForFilter = getBinNameForFilter(firstNameIdx, ageIdx, lastNameIdx);

        // creating an expression "firstName is equal to John"
        Qualifier firstNameEq = Qualifier.builder()
                .setPath("firstName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("John")
                .build();
        // creating an expression "age is equal to 42"
        Qualifier ageEq = Qualifier.builder()
                .setPath("age")
                .setFilterOperation(FilterOperation.EQ)
                .setValue(42)
                .build();
        // creating an expression "lastName is equal to Farmer"
        Qualifier lastNameEq = Qualifier.builder()
                .setPath("lastName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("Farmer")
                .build();

        Query query = new Query(Qualifier.and(firstNameEq, ageEq, lastNameEq));
        Filter filter = getQuerySecIndexFilter(query, IndexedPerson.class);
        assertThat(filter).isNotNull();
        assertThat(filter.getName()).isEqualTo(binNameChosenForFilter);

        Expression expression = getQueryExpression(query, IndexedPerson.class);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.and(
                                Exp.eq(Exp.bin("age", Exp.Type.INT), Exp.val((Integer) ageEq.getValue().getObject())),
                                Exp.eq(Exp.bin("lastName", Exp.Type.STRING), Exp.val((String) lastNameEq.getValue().getObject()))
                        )
                )
        );

        Iterable<IndexedPerson> result = repository.findUsingQuery(query);
        assertThat(result).containsOnly(john);
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
        String binNameChosenForFilter = getBinNameForFilter(firstNameIdx, ageIdx, lastNameIdx);

        // creating an expression "firstName is equal to John"
        Qualifier firstNameEq = Qualifier.builder()
                .setPath("firstName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("John")
                .build();
        // creating an expression "age is equal to 42"
        Qualifier ageEq = Qualifier.builder()
                .setPath("age")
                .setFilterOperation(FilterOperation.EQ)
                .setValue(42)
                .build();
        // creating an expression "lastName is equal to Farmer"
        Qualifier lastNameEq = Qualifier.builder()
                .setPath("lastName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("Farmer")
                .build();

        Query query = new Query(Qualifier.and(firstNameEq, Qualifier.and(ageEq, lastNameEq)));
        Filter filter = getQuerySecIndexFilter(query, IndexedPerson.class);
        assertThat(filter).isNotNull();
        assertThat(filter.getName()).isEqualTo(binNameChosenForFilter);

        Expression expression = getQueryExpression(query, IndexedPerson.class);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.and(
                                Exp.eq(Exp.bin("age", Exp.Type.INT), Exp.val((Integer) ageEq.getValue().getObject())),
                                Exp.eq(Exp.bin("lastName", Exp.Type.STRING), Exp.val((String) lastNameEq.getValue().getObject()))
                        )
                )
        );

        Iterable<IndexedPerson> result = repository.findUsingQuery(query);
        assertThat(result).containsOnly(john);
    }

    @Test
    void findBySimpleProperty_AND_OR() {
        Index firstNameIdx = indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "firstName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on firstName"));
        Index genderIdx = indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "gender", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on gender"));
        Index lastNameIdx = indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "lastName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on lastName"));
        // the cardinality of the corresponding index is higher (i.e. otherwise it would not have been chosen),
        // but based on the query only gender can be used for Filter because other bins are queried using OR
        String binNameChosenForFilter = "gender";


        // creating an expression "firstName is equal to John"
        Qualifier firstNameEq = Qualifier.builder()
                .setPath("firstName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("John")
                .build();
        // creating an expression "gender is equal to MALE"
        Qualifier genderEq = Qualifier.builder()
                .setPath("gender")
                .setFilterOperation(FilterOperation.EQ)
                .setValue(Person.Gender.MALE)
                .build();
        // creating an expression "lastName is equal to Macintosh"
        Qualifier lastNameEq = Qualifier.builder()
                .setPath("lastName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("Macintosh")
                .build();

        Query query = new Query(Qualifier.and(genderEq, Qualifier.or(firstNameEq, lastNameEq)));
        Filter filter = getQuerySecIndexFilter(query, IndexedPerson.class);
        assertThat(filter).isNotNull();
        assertThat(filter.getName()).isEqualTo(binNameChosenForFilter);

        Expression expression = getQueryExpression(query, IndexedPerson.class);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.or(
                                Exp.eq(Exp.bin("firstName", Exp.Type.STRING), Exp.val((String) firstNameEq.getValue().getObject())),
                                Exp.eq(Exp.bin("lastName", Exp.Type.STRING), Exp.val((String) lastNameEq.getValue().getObject()))
                        )
                )
        );

        Iterable<IndexedPerson> result = repository.findUsingQuery(query);
        assertThat(result).containsExactlyInAnyOrder(john, peter);
    }

    @Test
    void findBySimpleProperty_OR_AND() {
        Index firstNameIdx = indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "firstName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on firstName"));
        Index genderIdx = indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "gender", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on gender"));
        Index lastNameIdx = indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "lastName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on lastName"));

        // creating an expression "firstName is equal to John"
        Qualifier firstNameEq = Qualifier.builder()
                .setPath("firstName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("Peter")
                .build();
        // creating an expression "gender is equal to FEMALE"
        Qualifier genderEq = Qualifier.builder()
                .setPath("gender")
                .setFilterOperation(FilterOperation.EQ)
                .setValue(Person.Gender.FEMALE)
                .build();
        // creating an expression "lastName is equal to Macintosh"
        Qualifier lastNameEq = Qualifier.builder()
                .setPath("lastName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("Macintosh")
                .build();

        Query query = new Query(Qualifier.or(genderEq, Qualifier.and(firstNameEq, lastNameEq)));
        Filter filter = getQuerySecIndexFilter(query, IndexedPerson.class);
        // No single uniting secondary index Filter
        assertThat(filter).isNull();

        Expression expression = getQueryExpression(query, IndexedPerson.class);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.or(
                                Exp.eq(Exp.bin("gender", Exp.Type.STRING), Exp.val((String) genderEq.getValue().getObject())),
                                Exp.and(
                                        Exp.eq(Exp.bin("firstName", Exp.Type.STRING), Exp.val((String) firstNameEq.getValue().getObject())),
                                        Exp.eq(Exp.bin("lastName", Exp.Type.STRING), Exp.val((String) lastNameEq.getValue().getObject()))
                                )
                        )
                )
        );

        Iterable<IndexedPerson> result = repository.findUsingQuery(query);
        assertThat(result).containsExactlyInAnyOrder(peter, jane, tricia);
    }

    @Test
    void findBySimpleProperty_OR_OR() {
        Index firstNameIdx = indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "firstName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on firstName"));
        Index genderIdx = indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "gender", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on gender"));
        Index lastNameIdx = indexesCache.getIndex(
                new IndexKey(namespace, template.getSetName(IndexedPerson.class), "lastName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on lastName"));

        // creating an expression "firstName is equal to John"
        Qualifier firstNameEq = Qualifier.builder()
                .setPath("firstName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("John")
                .build();
        // creating an expression "gender is equal to FEMALE"
        Qualifier genderEq = Qualifier.builder()
                .setPath("gender")
                .setFilterOperation(FilterOperation.EQ)
                .setValue(Person.Gender.FEMALE)
                .build();
        // creating an expression "lastName is equal to Macintosh"
        Qualifier lastNameEq = Qualifier.builder()
                .setPath("lastName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("Macintosh")
                .build();

        Query query = new Query(Qualifier.or(genderEq, Qualifier.or(firstNameEq, lastNameEq)));
        Filter filter = getQuerySecIndexFilter(query, IndexedPerson.class);
        // No single uniting secondary index Filter
        assertThat(filter).isNull();

        Expression expression = getQueryExpression(query, IndexedPerson.class);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.or(
                                Exp.eq(Exp.bin("gender", Exp.Type.STRING), Exp.val((String) genderEq.getValue().getObject())),
                                Exp.or(
                                        Exp.eq(Exp.bin("firstName", Exp.Type.STRING), Exp.val((String) firstNameEq.getValue().getObject())),
                                        Exp.eq(Exp.bin("lastName", Exp.Type.STRING), Exp.val((String) lastNameEq.getValue().getObject()))
                                )
                        )
                )
        );

        Iterable<IndexedPerson> result = repository.findUsingQuery(query);
        assertThat(result).containsExactlyInAnyOrder(john, peter, jane, tricia);
    }
}
