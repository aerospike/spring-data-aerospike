package org.springframework.data.aerospike.query.reactive;

import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.repository.PersonTestData;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.aerospike.sample.ReactiveIndexedPersonRepository;
import reactor.core.scheduler.Schedulers;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.repository.PersonTestData.Indexed.alicia;
import static org.springframework.data.aerospike.repository.PersonTestData.Indexed.boyd;
import static org.springframework.data.aerospike.repository.PersonTestData.Indexed.carter;
import static org.springframework.data.aerospike.repository.PersonTestData.Indexed.dave;
import static org.springframework.data.aerospike.repository.PersonTestData.Indexed.donny;
import static org.springframework.data.aerospike.repository.PersonTestData.Indexed.leroi;
import static org.springframework.data.aerospike.repository.PersonTestData.Indexed.leroi2;
import static org.springframework.data.aerospike.repository.PersonTestData.Indexed.stefan;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReactiveIndexedPersonRepositoryQueryTests extends BaseReactiveIntegrationTests {

    @Autowired
    ReactiveIndexedPersonRepository reactiveRepository;

    @BeforeAll
    public void beforeAll() {
        reactiveRepository.deleteAll().block();

        reactiveRepository.saveAll(PersonTestData.Indexed.all).subscribeOn(Schedulers.parallel()).collectList().block();

        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_last_name_index", "lastName",
                IndexType.STRING)
            .block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_first_name_index", "firstName",
                IndexType.STRING)
            .block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_age_index", "age", IndexType.NUMERIC).block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_strings_index", "strings", IndexType.STRING
                , IndexCollectionType.LIST)
            .block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_ints_index", "ints", IndexType.NUMERIC,
                IndexCollectionType.LIST)
            .block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_string_map_keys_index", "stringMap",
                IndexType.STRING, IndexCollectionType.MAPKEYS)
            .block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_string_map_values_index", "stringMap",
                IndexType.STRING, IndexCollectionType.MAPVALUES)
            .block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_int_map_keys_index", "intMap",
                IndexType.STRING, IndexCollectionType.MAPKEYS)
            .block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_int_map_values_index", "intMap",
                IndexType.NUMERIC, IndexCollectionType.MAPVALUES)
            .block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_address_keys_index", "address",
                IndexType.STRING, IndexCollectionType.MAPKEYS)
            .block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_address_values_index", "address",
                IndexType.STRING, IndexCollectionType.MAPVALUES)
            .block();
    }

    @AfterAll
    public void afterAll() {
        additionalAerospikeTestOperations.deleteAllAndVerify(IndexedPerson.class);
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

        assertThat(results).containsExactly(dave);
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
