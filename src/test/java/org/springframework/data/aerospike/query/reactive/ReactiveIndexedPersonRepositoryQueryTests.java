package org.springframework.data.aerospike.query.reactive;

import com.aerospike.client.Value;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.ReactiveBlockingAerospikeTestOperations;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.MetadataQualifierBuilder;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.aerospike.query.QualifierBuilder;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.aerospike.sample.ReactiveIndexedPersonRepository;
import org.springframework.data.aerospike.utility.TestUtils;
import reactor.core.scheduler.Schedulers;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.AsCollections.of;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeMapCriteria.VALUE;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeMetadata.SINCE_UPDATE_TIME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReactiveIndexedPersonRepositoryQueryTests extends BaseReactiveIntegrationTests {

    @Autowired
    ReactiveIndexedPersonRepository reactiveRepository;
    static final IndexedPerson alain = IndexedPerson.builder().id(nextId()).firstName("Alain").lastName("Sebastian")
        .age(42).strings(Arrays.asList("str1", "str2"))
        .address(new Address("Foo Street 1", 1, "C0123", "Bar")).build();
    static final IndexedPerson luc = IndexedPerson.builder().id(nextId()).firstName("Luc").lastName("Besson").age(39)
        .stringMap(of("key1", "val1", "key2", "val2")).address(new Address(null, null, null, null))
        .build();
    static final IndexedPerson lilly = IndexedPerson.builder().id(nextId()).firstName("Lilly").lastName("Bertineau")
        .age(28).intMap(of("key1", 1, "key2", 2))
        .address(new Address("Foo Street 2", 2, "C0124", "C0123")).build();
    static final IndexedPerson daniel = IndexedPerson.builder().id(nextId()).firstName("Daniel").lastName("Morales")
        .age(29).ints(Arrays.asList(500, 550, 990)).build();
    static final IndexedPerson petra = IndexedPerson.builder().id(nextId()).firstName("Petra")
        .lastName("Coutant-Kerbalec")
        .age(34).stringMap(of("key1", "val1", "key2", "val2", "key3", "val3")).build();
    static final IndexedPerson emilien = IndexedPerson.builder().id(nextId()).firstName("Emilien")
        .lastName("Coutant-Kerbalec").age(30)
        .intMap(of("key1", 0, "key2", 1)).ints(Arrays.asList(450, 550, 990)).build();
    public static final List<IndexedPerson> allIndexedPersons = Arrays.asList(alain, luc, lilly, daniel, petra,
        emilien);
    @Autowired
    ReactiveBlockingAerospikeTestOperations reactiveBlockingAerospikeTestOperations;

    @BeforeAll
    public void beforeAll() {
        reactiveBlockingAerospikeTestOperations.deleteAll(reactiveRepository, allIndexedPersons);
        reactiveBlockingAerospikeTestOperations.saveAll(reactiveRepository, allIndexedPersons);
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_first_name_index", "firstName",
            IndexType.STRING).block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_last_name_index", "lastName",
            IndexType.STRING).block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_age_index", "age", IndexType.NUMERIC).block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_strings_index", "strings", IndexType.STRING
            , IndexCollectionType.LIST).block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_ints_index", "ints", IndexType.NUMERIC,
            IndexCollectionType.LIST).block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_string_map_keys_index", "stringMap",
            IndexType.STRING, IndexCollectionType.MAPKEYS).block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_string_map_values_index", "stringMap",
            IndexType.STRING, IndexCollectionType.MAPVALUES).block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_int_map_keys_index", "intMap",
            IndexType.STRING, IndexCollectionType.MAPKEYS).block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_int_map_values_index", "intMap",
            IndexType.NUMERIC, IndexCollectionType.MAPVALUES).block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_address_keys_index", "address",
            IndexType.STRING, IndexCollectionType.MAPKEYS).block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_address_values_index", "address",
            IndexType.STRING, IndexCollectionType.MAPVALUES).block();
    }

    @AfterAll
    public void afterAll() {
        reactiveBlockingAerospikeTestOperations.deleteAll(reactiveRepository, allIndexedPersons);

        additionalAerospikeTestOperations.dropIndex(IndexedPerson.class, "indexed_person_first_name_index");
        additionalAerospikeTestOperations.dropIndex(IndexedPerson.class, "indexed_person_last_name_index");
        additionalAerospikeTestOperations.dropIndex(IndexedPerson.class, "indexed_person_strings_index");
        additionalAerospikeTestOperations.dropIndex(IndexedPerson.class, "indexed_person_ints_index");
        additionalAerospikeTestOperations.dropIndex(IndexedPerson.class,
            "indexed_person_string_map_keys_index");
        additionalAerospikeTestOperations.dropIndex(IndexedPerson.class,
            "indexed_person_string_map_values_index");
        additionalAerospikeTestOperations.dropIndex(IndexedPerson.class, "indexed_person_int_map_keys_index");
        additionalAerospikeTestOperations.dropIndex(IndexedPerson.class, "indexed_person_int_map_values_index");
        additionalAerospikeTestOperations.dropIndex(IndexedPerson.class, "indexed_person_address_keys_index");
        additionalAerospikeTestOperations.dropIndex(IndexedPerson.class, "indexed_person_address_values_index");
    }

    @Test
    public void findByListContainingString_forExistingResult() {
        List<IndexedPerson> results = reactiveRepository.findByStringsContaining("str1")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactlyInAnyOrder(alain);
    }

    @Test
    public void findByListContainingInteger_forExistingResult() {
        List<IndexedPerson> results = reactiveRepository.findByIntsContaining(550)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactlyInAnyOrder(daniel, emilien);
    }

    @Test
    public void findByListValueGreaterThan() {
        List<IndexedPerson> results = reactiveRepository.findByIntsGreaterThan(549)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactlyInAnyOrder(daniel, emilien);
    }

    @Test
    public void findByListValueLessThanOrEqual() {
        List<IndexedPerson> results = reactiveRepository.findByIntsLessThanEqual(500)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactlyInAnyOrder(daniel, emilien);
    }

    @Test
    public void findByListValueInRange() {
        List<IndexedPerson> results = reactiveRepository.findByIntsBetween(500, 600)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactlyInAnyOrder(daniel, emilien);
    }

    @Test
    public void findPersonsByLastname() {
        List<IndexedPerson> results = reactiveRepository.findByLastName("Coutant-Kerbalec")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(petra, emilien);
    }

    @Test
    public void findPersonsByFirstname() {
        List<IndexedPerson> results = reactiveRepository.findByFirstName("Lilly")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactlyInAnyOrder(lilly);
    }

    @Test
    void findDistinctByLastNameStartingWith() {
        List<IndexedPerson> persons = reactiveRepository.findDistinctByLastNameStartingWith("Coutant-Kerbalec")
            .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(persons).hasSize(1);

        List<IndexedPerson> persons2 = reactiveRepository.findByLastNameStartingWith("Coutant-Kerbalec")
            .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(persons2).hasSize(2);
    }

    @Test
    void findDistinctByFriendLastNameStartingWith() {
        alain.setFriend(luc);
        reactiveRepository.save(alain);
        lilly.setFriend(petra);
        reactiveRepository.save(lilly);
        daniel.setFriend(emilien);
        reactiveRepository.save(daniel);

        assertThatThrownBy(() -> reactiveRepository.findDistinctByFriendLastNameStartingWith("l"))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage("DISTINCT queries are currently supported only for the first level objects, got a query for " +
                "friend.lastName");

        TestUtils.setFriendsToNull(reactiveRepository, alain, lilly, daniel);
    }

    @Test
    public void findPersonsByFirstnameAndByAge() {
        List<IndexedPerson> results = reactiveRepository.findByFirstNameAndAge("Lilly", 28)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(lilly);
    }

    @Test
    public void findPersonInAgeRangeCorrectly() {
        List<IndexedPerson> results = reactiveRepository.findByAgeBetween(39, 45)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).hasSize(2).contains(alain, luc);
    }

    @Test
    public void findByMapKeysContaining() {
        List<IndexedPerson> results = reactiveRepository.findByStringMapContaining("key1",
            CriteriaDefinition.AerospikeMapCriteria.KEY).subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).contains(luc, petra);
    }

    @Test
    public void findByMapValuesContaining() {
        List<IndexedPerson> results = reactiveRepository.findByStringMapContaining("val1",
                VALUE)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).contains(luc, petra);
    }

    @Test
    public void findByMapKeyValueContainingString() {
        assertThat(petra.getStringMap().containsKey("key1")).isTrue();
        assertThat(petra.getStringMap().containsValue("val1")).isTrue();
        assertThat(luc.getStringMap().containsKey("key1")).isTrue();
        assertThat(luc.getStringMap().containsValue("val1")).isTrue();

        List<IndexedPerson> results = reactiveRepository.findByStringMapContaining("key1", "val1")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).contains(petra, luc);
    }

    @Test
    public void findPersonsByAddressZipCode() {
        String zipCode = "C0123";
        assertThat(alain.getAddress().getZipCode()).isEqualTo(zipCode);

        List<IndexedPerson> results = reactiveRepository.findByAddressZipCode(zipCode)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).contains(alain);
    }

    @Test
    public void findByMapKeyValueContainingInt() {
        assertThat(emilien.getIntMap().containsKey("key1")).isTrue();
        assertThat(emilien.getIntMap().get("key1") == 0).isTrue();
        assertThat(lilly.getIntMap().containsKey("key1")).isTrue();
        assertThat(lilly.getIntMap().get("key1") == 0).isFalse();

        List<IndexedPerson> results = reactiveRepository.findByIntMapContaining("key1", 0)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactly(emilien);
    }

    @Test
    public void findByMapKeyValueGreaterThan() {
        assertThat(emilien.getIntMap().containsKey("key2")).isTrue();
        assertThat(emilien.getIntMap().get("key2") > 0).isTrue();
        assertThat(lilly.getIntMap().containsKey("key2")).isTrue();
        assertThat(lilly.getIntMap().get("key2") > 0).isTrue();

        List<IndexedPerson> results = reactiveRepository.findByIntMapGreaterThan("key2", 0)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactlyInAnyOrder(emilien, lilly);
    }

    @Test
    public void findByMapKeyValueLessThanOrEqual() {
        assertThat(emilien.getIntMap().containsKey("key2")).isTrue();
        assertThat(emilien.getIntMap().get("key2") <= 1).isTrue();
        assertThat(lilly.getIntMap().containsKey("key2")).isTrue();
        assertThat(lilly.getIntMap().get("key2") <= 1).isFalse();

        List<IndexedPerson> results = reactiveRepository.findByIntMapLessThanEqual("key2", 1)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactlyInAnyOrder(emilien);
    }

    public void findByMapKeyValueBetween() {
        assertThat(lilly.getIntMap().containsKey("key2")).isTrue();
        assertThat(emilien.getIntMap().containsKey("key2")).isTrue();
        assertThat(lilly.getIntMap().get("key2") >= 0).isTrue();
        assertThat(emilien.getIntMap().get("key2") >= 0).isTrue();

        List<IndexedPerson> results = reactiveRepository.findByIntMapBetween("key2", 0, 1)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactlyInAnyOrder(lilly, emilien);
    }

    @Test
    public void findPersonsByMetadata() {
        // creating a condition "since_update_time metadata value is less than 50 seconds"
        Qualifier sinceUpdateTimeLt10Seconds = new MetadataQualifierBuilder()
            .setMetadataField(SINCE_UPDATE_TIME)
            .setFilterOperation(FilterOperation.LT)
            .setValue1AsObj(50000L)
            .build();
        assertThat(reactiveRepository.findByQualifiers(sinceUpdateTimeLt10Seconds).collectList().block())
            .containsAll(allIndexedPersons);

        // creating a condition "since_update_time metadata value is between 1 millisecond and 50 seconds"
        Qualifier sinceUpdateTimeBetween1And50000 = new MetadataQualifierBuilder()
            .setMetadataField(SINCE_UPDATE_TIME)
            .setFilterOperation(FilterOperation.BETWEEN)
            .setValue1AsObj(1L)
            .setValue2AsObj(50000L)
            .build();
        assertThat(reactiveRepository.findByQualifiers(sinceUpdateTimeBetween1And50000).collectList().block())
            .containsAll(reactiveRepository.findByQualifiers(sinceUpdateTimeLt10Seconds).collectList().block());
    }

    @Test
    public void findPersonsByQualifiers() {
        Iterable<IndexedPerson> result;

        // creating a condition "since_update_time metadata value is greater than 1 millisecond"
        Qualifier sinceUpdateTimeGt1 = new MetadataQualifierBuilder()
            .setMetadataField(SINCE_UPDATE_TIME)
            .setFilterOperation(FilterOperation.GT)
            .setValue1AsObj(1L)
            .build();

        // creating a condition "since_update_time metadata value is less than 50 seconds"
        Qualifier sinceUpdateTimeLt50Seconds = new MetadataQualifierBuilder()
            .setMetadataField(SINCE_UPDATE_TIME)
            .setFilterOperation(FilterOperation.LT)
            .setValue1AsObj(50000L)
            .build();
        assertThat(reactiveRepository.findByQualifiers(sinceUpdateTimeLt50Seconds).collectList().block())
            .containsAll(allIndexedPersons);

        // creating a condition "since_update_time metadata value is between 1 and 50 seconds"
        Qualifier sinceUpdateTimeBetween1And50000 = new MetadataQualifierBuilder()
            .setMetadataField(SINCE_UPDATE_TIME)
            .setFilterOperation(FilterOperation.BETWEEN)
            .setValue1AsObj(1L)
            .setValue2AsObj(50000L)
            .build();

        // creating a condition "firsName is equal to Petra"
        Qualifier firstNameEqPetra = new QualifierBuilder()
            .setField("firstName")
            .setFilterOperation(FilterOperation.EQ)
            .setValue1(Value.get("Petra"))
            .build();

        // creating a condition "age is equal to 34"
        Qualifier ageEq34 = new QualifierBuilder()
            .setField("age")
            .setFilterOperation(FilterOperation.EQ)
            .setValue1(Value.get(34))
            .build();
        result = reactiveRepository.findByQualifiers(ageEq34).collectList().block();
        assertThat(result).containsOnly(petra);

        // creating a condition "age is greater than 34"
        Qualifier ageGt34 = new QualifierBuilder()
            .setFilterOperation(FilterOperation.GT)
            .setField("age")
            .setValue1(Value.get(34))
            .build();
        result = reactiveRepository.findByQualifiers(ageGt34).collectList().block();
        assertThat(result).doesNotContain(petra);

        // default conjunction for multiple qualifiers given to "findByMetadata" is AND
        result = reactiveRepository.findByQualifiers(sinceUpdateTimeGt1, sinceUpdateTimeLt50Seconds, ageEq34,
            firstNameEqPetra,
            sinceUpdateTimeBetween1And50000).collectList().block();
        assertThat(result).containsOnly(petra);

        // conditions "age == 34", "firstName is Petra" and "since_update_time metadata value is less than 50 seconds"
        // are combined with OR
        Qualifier orWide = new QualifierBuilder()
            .setFilterOperation(FilterOperation.OR)
            .setQualifiers(ageEq34, firstNameEqPetra, sinceUpdateTimeLt50Seconds)
            .build();
        result = reactiveRepository.findByQualifiers(orWide).collectList().block();
        assertThat(result).containsAll(allIndexedPersons);

        // conditions "age == 34" and "firstName is Petra" are combined with OR
        Qualifier orNarrow = new QualifierBuilder()
            .setFilterOperation(FilterOperation.OR)
            .setQualifiers(ageEq34, firstNameEqPetra)
            .build();
        result = reactiveRepository.findByQualifiers(orNarrow).collectList().block();
        assertThat(result).containsOnly(petra);

        result = reactiveRepository.findByQualifiers(new QualifierBuilder()
            .setFilterOperation(FilterOperation.AND)
            .setQualifiers(ageEq34, ageGt34)
            .build()).collectList().block();
        assertThat(result).isEmpty();

        // default conjunction for multiple qualifiers given to "findByMetadata" is AND
        // conditions "age == 34" and "age > 34" are not overlapping
        result = reactiveRepository.findByQualifiers(ageEq34, ageGt34).collectList().block();
        assertThat(result).isEmpty();

        // conditions "age == 34" and "age > 34" are combined with OR
        Qualifier ageEqOrGt34 = new QualifierBuilder()
            .setFilterOperation(FilterOperation.OR)
            .setQualifiers(ageEq34, ageGt34)
            .build();

        result = reactiveRepository.findByQualifiers(ageEqOrGt34).collectList().block();
        List<IndexedPerson> personsWithAgeEqOrGt34 = allIndexedPersons.stream().filter(person -> person.getAge() >= 34)
            .toList();
        assertThat(result).containsAll(personsWithAgeEqOrGt34);

        // a condition that returns all entities and a condition that returns one entity are combined using AND
        result = reactiveRepository.findByQualifiers(orWide, orNarrow).collectList().block();
        assertThat(result).containsOnly(petra);

        // a condition that returns all entities and a condition that returns one entity are combined using AND
        // another way of running the same query
        Qualifier orCombinedWithAnd = new QualifierBuilder()
            .setFilterOperation(FilterOperation.AND)
            .setQualifiers(orWide, orNarrow)
            .build();
        result = reactiveRepository.findByQualifiers(orCombinedWithAnd).collectList().block();
        assertThat(result).containsOnly(petra);
    }
}
