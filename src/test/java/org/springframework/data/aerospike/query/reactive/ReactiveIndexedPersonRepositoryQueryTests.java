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
import reactor.core.scheduler.Schedulers;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.AsCollections.of;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReactiveIndexedPersonRepositoryQueryTests extends BaseReactiveIntegrationTests {

    @Autowired
    ReactiveIndexedPersonRepository reactiveRepository;
    static final IndexedPerson dave = IndexedPerson.builder().id(nextId()).firstName("Dave").lastName("Matthews")
        .age(42).strings(Arrays.asList("str1", "str2"))
        .address(new Address("Foo Street 1", 1, "C0123", "Bar")).build();
    static final IndexedPerson donny = IndexedPerson.builder().id(nextId()).firstName("Donny").lastName("Macintire")
        .age(39).strings(Arrays.asList("str1", "str2", "str3")).build();
    static final IndexedPerson oliver = IndexedPerson.builder().id(nextId()).firstName("Oliver August")
        .lastName("Matthews").age(14).build();
    static final IndexedPerson carter = IndexedPerson.builder().id(nextId()).firstName("Carter").lastName("Beauford")
        .age(49).intMap(of("key1", 0, "key2", 1))
        .address(new Address("Foo Street 2", 2, "C0124", "C0123")).build();
    static final IndexedPerson boyd = IndexedPerson.builder().id(nextId()).firstName("Boyd").lastName("Tinsley").age(45)
        .stringMap(of("key1", "val1", "key2", "val2")).address(new Address(null, null, null, null))
        .build();
    static final IndexedPerson stefan = IndexedPerson.builder().id(nextId()).firstName("Stefan").lastName("Lessard")
        .age(34).stringMap(of("key1", "val1", "key2", "val2", "key3", "val3")).build();
    static final IndexedPerson leroi = IndexedPerson.builder().id(nextId()).firstName("Leroi").lastName("Moore").age(41)
        .intMap(of("key1", 0, "key2", 1)).build();
    static final IndexedPerson leroi2 = IndexedPerson.builder().id(nextId()).firstName("Leroi").lastName("Moore")
        .age(25).ints(Arrays.asList(500, 550, 990)).build();
    static final IndexedPerson alicia = IndexedPerson.builder().id(nextId()).firstName("Alicia").lastName("Keys")
        .age(30).ints(Arrays.asList(550, 600, 990)).build();
    static final IndexedPerson matias = IndexedPerson.builder().id(nextId()).firstName("Matias").lastName("Craft")
        .age(24).build();
    static final IndexedPerson douglas = IndexedPerson.builder().id(nextId()).firstName("Douglas").lastName("Ford")
        .age(25).build();
    public static final List<IndexedPerson> allIndexedPersons = Arrays.asList(oliver, dave, donny, carter, boyd,
        stefan, leroi, leroi2, alicia, matias, douglas);

    @BeforeAll
    public void beforeAll() {
        reactiveRepository.deleteAll().block();

        reactiveRepository.saveAll(allIndexedPersons).subscribeOn(Schedulers.parallel()).collectList().block();

        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_last_name_index", "lastName",
            IndexType.STRING).block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_first_name_index", "firstName",
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
        additionalAerospikeTestOperations.deleteAllAndVerify(IndexedPerson.class);
        additionalAerospikeTestOperations.dropIndexIfExists(IndexedPerson.class, "indexed_person_last_name_index");
        additionalAerospikeTestOperations.dropIndexIfExists(IndexedPerson.class, "indexed_person_first_name_index");
        additionalAerospikeTestOperations.dropIndexIfExists(IndexedPerson.class, "indexed_person_strings_index");
        additionalAerospikeTestOperations.dropIndexIfExists(IndexedPerson.class, "indexed_person_ints_index");
        additionalAerospikeTestOperations.dropIndexIfExists(IndexedPerson.class,
            "indexed_person_string_map_keys_index");
        additionalAerospikeTestOperations.dropIndexIfExists(IndexedPerson.class,
            "indexed_person_string_map_values_index");
        additionalAerospikeTestOperations.dropIndexIfExists(IndexedPerson.class, "indexed_person_int_map_keys_index");
        additionalAerospikeTestOperations.dropIndexIfExists(IndexedPerson.class, "indexed_person_int_map_values_index");
        additionalAerospikeTestOperations.dropIndexIfExists(IndexedPerson.class, "indexed_person_address_keys_index");
        additionalAerospikeTestOperations.dropIndexIfExists(IndexedPerson.class, "indexed_person_address_values_index");
        reactorIndexRefresher.clearCache();
    }

    @Test
    public void findByListContainingString_forExistingResult() {
        List<IndexedPerson> results = reactiveRepository.findByStringsContaining("str1")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactlyInAnyOrder(dave, donny);
    }

    @Test
    public void findByListContainingInteger_forExistingResult() {
        List<IndexedPerson> results = reactiveRepository.findByIntsContaining(550)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactlyInAnyOrder(leroi2, alicia);
    }

    @Test
    public void findByListValueGreaterThan() {
        List<IndexedPerson> results = reactiveRepository.findByIntsGreaterThan(549)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactlyInAnyOrder(leroi2, alicia);
    }

    @Test
    public void findByListValueLessThanOrEqual() {
        List<IndexedPerson> results = reactiveRepository.findByIntsLessThanEqual(500)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactlyInAnyOrder(leroi2);
    }

    @Test
    public void findByListValueInRange() {
        List<IndexedPerson> results = reactiveRepository.findByIntsBetween(500, 600)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactlyInAnyOrder(leroi2, alicia);
    }

    @Test
    public void findsPersonsByLastname() {
        List<IndexedPerson> results = reactiveRepository.findByLastName("Beauford")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(carter);
    }

    @Test
    public void findsPersonsByFirstname() {
        List<IndexedPerson> results = reactiveRepository.findByFirstName("Leroi")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactlyInAnyOrder(leroi, leroi2);
    }

    @Test
    public void findsPersonsByFirstnameAndByAge() {
        List<IndexedPerson> results = reactiveRepository.findByFirstNameAndAge("Leroi", 25)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(leroi2);
    }

    @Test
    public void findsPersonInAgeRangeCorrectly() {
        List<IndexedPerson> results = reactiveRepository.findByAgeBetween(40, 45)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).hasSize(3).contains(dave);
    }

    @Test
    public void findByMapKeysContaining() {
        List<IndexedPerson> results = reactiveRepository.findByStringMapContaining("key1",
                CriteriaDefinition.AerospikeMapCriteria.KEY)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).contains(stefan, boyd);
    }

    @Test
    public void findByMapValuesContaining() {
        List<IndexedPerson> results = reactiveRepository.findByStringMapContaining("val1",
                CriteriaDefinition.AerospikeMapCriteria.VALUE)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).contains(stefan, boyd);
    }

    @Test
    public void findByMapKeyValueEqualsInt() {
        assertThat(leroi.getIntMap().containsKey("key1")).isTrue();
        assertThat(leroi.getIntMap().containsValue(0)).isTrue();

        List<IndexedPerson> results = reactiveRepository.findByIntMapEquals("key1", 0)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactlyInAnyOrder(carter, leroi);
    }

    @Test
    public void findByMapKeyValueEqualsString() {
        assertThat(stefan.getStringMap().containsKey("key1")).isTrue();
        assertThat(stefan.getStringMap().containsValue("val1")).isTrue();
        assertThat(boyd.getStringMap().containsKey("key1")).isTrue();
        assertThat(boyd.getStringMap().containsValue("val1")).isTrue();

        List<IndexedPerson> results = reactiveRepository.findByStringMapEquals("key1", "val1")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).contains(stefan, boyd);
    }

    @Test
    public void findPersonsByAddressZipCode() {
        String zipCode = "C0123";
        assertThat(dave.getAddress().getZipCode()).isEqualTo(zipCode);

        List<IndexedPerson> results = reactiveRepository.findByAddressZipCode(zipCode)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).contains(dave);
    }

    @Test
    public void findByMapKeyValueGreaterThan() {
        assertThat(leroi.getIntMap().containsKey("key2")).isTrue();
        assertThat(leroi.getIntMap().get("key2") > 0).isTrue();

        List<IndexedPerson> results = reactiveRepository.findByIntMapGreaterThan("key2", 0)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).contains(leroi);
    }

    @Test
    public void findByMapKeyValueLessThanOrEqual() {
        assertThat(leroi.getIntMap().containsKey("key2")).isTrue();
        assertThat(leroi.getIntMap().get("key2") > 0).isTrue();

        List<IndexedPerson> results = reactiveRepository.findByIntMapLessThanEqual("key2", 1)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactlyInAnyOrder(leroi, carter);
    }

    public void findByMapKeyValueBetween() {
        assertThat(carter.getIntMap().containsKey("key2")).isTrue();
        assertThat(leroi.getIntMap().containsKey("key2")).isTrue();
        assertThat(carter.getIntMap().get("key2") >= 0).isTrue();
        assertThat(leroi.getIntMap().get("key2") >= 0).isTrue();

        List<IndexedPerson> results = reactiveRepository.findByIntMapBetween("key2", 0, 1)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactlyInAnyOrder(leroi, carter);
    }
}
