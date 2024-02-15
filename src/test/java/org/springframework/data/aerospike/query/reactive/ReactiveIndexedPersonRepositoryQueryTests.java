package org.springframework.data.aerospike.query.reactive;

import com.aerospike.client.Value;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.AsCollections;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.ReactiveBlockingAerospikeTestOperations;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.aerospike.sample.ReactiveIndexedPersonRepository;
import org.springframework.data.aerospike.utility.TestUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import reactor.core.scheduler.Schedulers;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeMetadata.SINCE_UPDATE_TIME;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriteria.VALUE;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReactiveIndexedPersonRepositoryQueryTests extends BaseReactiveIntegrationTests {

    static final IndexedPerson alain = IndexedPerson.builder().id(nextId()).firstName("Alain").lastName("Sebastian")
        .age(42).strings(Arrays.asList("str1", "str2"))
        .address(new Address("Foo Street 1", 1, "C0123", "Bar")).build();
    static final IndexedPerson luc = IndexedPerson.builder().id(nextId()).firstName("Luc").lastName("Besson").age(39)
        .stringMap(AsCollections.of("key1", "val1", "key2", "val2")).address(new Address(null, null, null, null))
        .build();
    static final IndexedPerson lilly = IndexedPerson.builder().id(nextId()).firstName("Lilly").lastName("Bertineau")
        .age(28).intMap(AsCollections.of("key1", 1, "key2", 2))
        .address(new Address("Foo Street 2", 2, "C0124", "C0123")).build();
    static final IndexedPerson daniel = IndexedPerson.builder().id(nextId()).firstName("Daniel").lastName("Morales")
        .age(29).ints(Arrays.asList(500, 550, 990)).build();
    static final IndexedPerson petra = IndexedPerson.builder().id(nextId()).firstName("Petra")
        .lastName("Coutant-Kerbalec")
        .age(34).stringMap(AsCollections.of("key1", "val1", "key2", "val2", "key3", "val3")).build();
    static final IndexedPerson emilien = IndexedPerson.builder().id(nextId()).firstName("Emilien")
        .lastName("Coutant-Kerbalec").age(30)
        .intMap(AsCollections.of("key1", 0, "key2", 1)).ints(Arrays.asList(450, 550, 990)).build();
    public static final List<IndexedPerson> allIndexedPersons = Arrays.asList(alain, luc, lilly, daniel, petra,
        emilien);
    @Autowired
    ReactiveIndexedPersonRepository reactiveRepository;
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
    public void findByPaginatedQuery_forExistingResult() {
        Page<IndexedPerson> page = reactiveRepository.findByAgeGreaterThan(1, PageRequest.of(0, 1))
            .subscribeOn(Schedulers.parallel()).block();
        assertThat(page).containsAnyElementsOf(allIndexedPersons);

        Slice<IndexedPerson> slice = reactiveRepository.findByAgeGreaterThan(1, PageRequest.of(0, 2))
            .subscribeOn(Schedulers.parallel()).block();
        assertThat(slice).hasSize(2).containsAnyElementsOf(allIndexedPersons);

        Slice<IndexedPerson> sliceSorted = reactiveRepository.findByAgeGreaterThan(1, PageRequest.of(1, 2, Sort.by(
                "age")))
            .subscribeOn(Schedulers.parallel()).block();
        assertThat(sliceSorted).hasSize(2).containsAnyElementsOf(allIndexedPersons);

        assertThatThrownBy(() -> reactiveRepository.findByAgeGreaterThan(1, PageRequest.of(1, 2)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unsorted query must not have offset value. For retrieving paged results use sorted query.");
    }

    @Test
    public void findByAgeGreaterThanWithPageableUnpaged() {
        Slice<IndexedPerson> slice = reactiveRepository.findByAgeGreaterThan(40, Pageable.unpaged())
            .subscribeOn(Schedulers.parallel()).block();
        assertThat(slice.hasContent()).isTrue();
        assertThat(slice.getNumberOfElements()).isGreaterThan(0);
        assertThat(slice.hasNext()).isFalse();
        assertThat(slice.isLast()).isTrue();

        Page<IndexedPerson> page = reactiveRepository.findByAgeLessThan(40, Pageable.unpaged())
            .subscribeOn(Schedulers.parallel()).block();
        assertThat(page.hasContent()).isTrue();
        assertThat(page.getNumberOfElements()).isGreaterThan(1);
        assertThat(page.hasNext()).isFalse();
        assertThat(page.getTotalPages()).isEqualTo(1);
        assertThat(page.getTotalElements()).isEqualTo(page.getSize());
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
        QueryParam firstName = QueryParam.of("Lilly");
        QueryParam age = QueryParam.of(28);
        List<IndexedPerson> results = reactiveRepository.findByFirstNameAndAge(firstName, age)
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
            CriteriaDefinition.AerospikeQueryCriteria.KEY).subscribeOn(Schedulers.parallel()).collectList().block();

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
        assertThat(emilien.getIntMap().get("key1")).isZero();
        assertThat(lilly.getIntMap().containsKey("key1")).isTrue();
        assertThat(lilly.getIntMap().get("key1")).isNotZero();

        List<IndexedPerson> results = reactiveRepository.findByIntMapContaining("key1", 0)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactly(emilien);
    }

    @Test
    public void findByMapKeyValueGreaterThan() {
        assertThat(emilien.getIntMap().containsKey("key2")).isTrue();
        assertThat(emilien.getIntMap().get("key2")).isPositive();
        assertThat(lilly.getIntMap().containsKey("key2")).isTrue();
        assertThat(lilly.getIntMap().get("key2")).isPositive();

        List<IndexedPerson> results = reactiveRepository.findByIntMapGreaterThan("key2", 0)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactlyInAnyOrder(emilien, lilly);
    }

    @Test
    public void findByMapKeyValueLessThanOrEqual() {
        assertThat(emilien.getIntMap().containsKey("key2")).isTrue();
        assertThat(emilien.getIntMap().get("key2")).isLessThanOrEqualTo(1);
        assertThat(lilly.getIntMap().containsKey("key2")).isTrue();
        assertThat(lilly.getIntMap().get("key2")).isGreaterThan(1);

        List<IndexedPerson> results = reactiveRepository.findByIntMapLessThanEqual("key2", 1)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactlyInAnyOrder(emilien);
    }

    public void findByMapKeyValueBetween() {
        assertThat(lilly.getIntMap().containsKey("key2")).isTrue();
        assertThat(emilien.getIntMap().containsKey("key2")).isTrue();
        assertThat(lilly.getIntMap().get("key2")).isNotNegative();
        assertThat(emilien.getIntMap().get("key2")).isNotNegative();

        List<IndexedPerson> results = reactiveRepository.findByIntMapBetween("key2", 0, 1)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactlyInAnyOrder(lilly, emilien);
    }

    @Test
    public void findPersonsByMetadata() {
        // creating an expression "since_update_time metadata value is less than 50 seconds"
        Qualifier sinceUpdateTimeLt10Seconds = Qualifier.metadataBuilder()
            .setMetadataField(SINCE_UPDATE_TIME)
            .setFilterOperation(FilterOperation.LT)
            .setValue1AsObj(50000L)
            .build();
        assertThat(reactiveRepository.findUsingQuery(new Query(sinceUpdateTimeLt10Seconds)).collectList().block())
            .containsAll(allIndexedPersons);

        // creating an expression "since_update_time metadata value is between 1 millisecond and 50 seconds"
        Qualifier sinceUpdateTimeBetween1And50000 = Qualifier.metadataBuilder()
            .setMetadataField(SINCE_UPDATE_TIME)
            .setFilterOperation(FilterOperation.BETWEEN)
            .setValue1AsObj(1L)
            .setValue2AsObj(50000L)
            .build();
        assertThat(reactiveRepository.findUsingQuery(new Query(sinceUpdateTimeBetween1And50000)).collectList().block())
            .containsAll(reactiveRepository.findUsingQuery(new Query(sinceUpdateTimeLt10Seconds)).collectList()
                .block());
    }

    @Test
    public void findPersonsByQuery() {
        Iterable<IndexedPerson> result;

        // creating an expression "since_update_time metadata value is greater than 1 millisecond"
        Qualifier sinceUpdateTimeGt1 = Qualifier.metadataBuilder()
            .setMetadataField(SINCE_UPDATE_TIME)
            .setFilterOperation(FilterOperation.GT)
            .setValue1AsObj(1L)
            .build();

        // creating an expression "since_update_time metadata value is less than 50 seconds"
        Qualifier sinceUpdateTimeLt50Seconds = Qualifier.metadataBuilder()
            .setMetadataField(SINCE_UPDATE_TIME)
            .setFilterOperation(FilterOperation.LT)
            .setValue1AsObj(50000L)
            .build();
        assertThat(reactiveRepository.findUsingQuery(new Query(sinceUpdateTimeLt50Seconds)).collectList().block())
            .containsAll(allIndexedPersons);

        // creating an expression "since_update_time metadata value is between 1 and 50 seconds"
        Qualifier sinceUpdateTimeBetween1And50000 = Qualifier.metadataBuilder()
            .setMetadataField(SINCE_UPDATE_TIME)
            .setFilterOperation(FilterOperation.BETWEEN)
            .setValue1AsObj(1L)
            .setValue2AsObj(50000L)
            .build();

        // creating an expression "firsName is equal to Petra"
        Qualifier firstNameEqPetra = Qualifier.builder()
            .setField("firstName")
            .setFilterOperation(FilterOperation.EQ)
            .setValue1(Value.get("Petra"))
            .build();

        // creating an expression "age is equal to 34"
        Qualifier ageEq34 = Qualifier.builder()
            .setField("age")
            .setFilterOperation(FilterOperation.EQ)
            .setValue1(Value.get(34))
            .build();
        result = reactiveRepository.findUsingQuery(new Query(ageEq34)).collectList().block();
        assertThat(result).containsOnly(petra);

        // creating an expression "age is greater than 34"
        Qualifier ageGt34 = Qualifier.builder()
            .setFilterOperation(FilterOperation.GT)
            .setField("age")
            .setValue1(Value.get(34))
            .build();
        result = reactiveRepository.findUsingQuery(new Query(ageGt34)).collectList().block();
        assertThat(result).doesNotContain(petra);

        result = reactiveRepository.findUsingQuery(new Query(Qualifier.and(sinceUpdateTimeGt1,
            sinceUpdateTimeLt50Seconds,
            ageEq34,
            firstNameEqPetra, sinceUpdateTimeBetween1And50000))).collectList().block();
        assertThat(result).containsOnly(petra);

        // conditions "age == 34", "firstName is Petra" and "since_update_time metadata value is less than 50 seconds"
        // are combined with OR
        Qualifier orWide = Qualifier.or(ageEq34, firstNameEqPetra, sinceUpdateTimeLt50Seconds);
        result = reactiveRepository.findUsingQuery(new Query(orWide)).collectList().block();
        assertThat(result).containsAll(allIndexedPersons);

        // conditions "age == 34" and "firstName is Petra" are combined with OR
        Qualifier orNarrow = Qualifier.or(ageEq34, firstNameEqPetra);
        result = reactiveRepository.findUsingQuery(new Query(orNarrow)).collectList().block();
        assertThat(result).containsOnly(petra);

        result = reactiveRepository.findUsingQuery(new Query(Qualifier.and(ageEq34, ageGt34))).collectList().block();
        assertThat(result).isEmpty();

        // conditions "age == 34" and "age > 34" are not overlapping
        result = reactiveRepository.findUsingQuery(new Query(Qualifier.and(ageEq34, ageGt34))).collectList().block();
        assertThat(result).isEmpty();

        // conditions "age == 34" and "age > 34" are combined with OR
        Qualifier ageEqOrGt34 = Qualifier.or(ageEq34, ageGt34);

        result = reactiveRepository.findUsingQuery(new Query(ageEqOrGt34)).collectList().block();
        List<IndexedPerson> personsWithAgeEqOrGt34 = allIndexedPersons.stream().filter(person -> person.getAge() >= 34)
            .toList();
        assertThat(result).containsAll(personsWithAgeEqOrGt34);

        // a condition that returns all entities and a condition that returns one entity are combined using AND
        result = reactiveRepository.findUsingQuery(new Query(Qualifier.and(orWide, orNarrow))).collectList().block();
        assertThat(result).containsOnly(petra);

        // a condition that returns all entities and a condition that returns one entity are combined using AND
        // another way of running the same query
        Qualifier orCombinedWithAnd = Qualifier.and(orWide, orNarrow);
        result = reactiveRepository.findUsingQuery(new Query(orCombinedWithAnd)).collectList().block();
        assertThat(result).containsOnly(petra);
    }
}
