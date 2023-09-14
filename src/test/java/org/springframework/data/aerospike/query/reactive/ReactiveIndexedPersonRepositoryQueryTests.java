package org.springframework.data.aerospike.query.reactive;

import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
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

    @BeforeAll
    public void beforeAll() {
        reactiveRepository.deleteAll(allIndexedPersons).block();
        reactiveRepository.saveAll(allIndexedPersons).subscribeOn(Schedulers.parallel()).collectList().block();
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
        reactorIndexRefresher.refreshIndexes();
    }

    @AfterAll
    public void afterAll() {
        reactiveRepository.deleteAll(allIndexedPersons).block();
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
        reactorIndexRefresher.refreshIndexes();
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
}
