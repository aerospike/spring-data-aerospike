package org.springframework.data.aerospike.repository.query.reactive.indexed;

import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.AsCollections;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.ReactiveBlockingAerospikeTestOperations;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.aerospike.sample.ReactiveIndexedPersonRepository;

import java.util.Arrays;
import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReactiveIndexedPersonRepositoryQueryTests extends BaseReactiveIntegrationTests {

    protected static final IndexedPerson alain = IndexedPerson.builder()
        .id(nextId())
        .firstName("Alain")
        .lastName("Sebastian")
        .age(42)
        .strings(Arrays.asList("str1", "str2"))
        .address(new Address("Foo Street 1", 1, "C0123", "Bar"))
        .build();
    protected static final IndexedPerson luc = IndexedPerson.builder()
        .id(nextId())
        .firstName("Luc")
        .lastName("Besson")
        .age(39)
        .stringMap(AsCollections.of("key1", "val1", "key2", "val2"))
        .address(new Address(null, null, null, null))
        .build();
    protected static final IndexedPerson lilly = IndexedPerson.builder()
        .id(nextId())
        .firstName("Lilly")
        .lastName("Bertineau")
        .age(28)
        .intMap(AsCollections.of("key1", 1, "key2", 2))
        .address(new Address("Foo Street 2", 2, "C0124", "C0123"))
        .build();
    protected static final IndexedPerson daniel = IndexedPerson.builder()
        .id(nextId())
        .firstName("Daniel")
        .lastName("Morales")
        .age(29)
        .ints(Arrays.asList(500, 550, 990))
        .build();
    protected static final IndexedPerson petra = IndexedPerson.builder()
        .id(nextId())
        .firstName("Petra")
        .lastName("Coutant-Kerbalec")
        .age(34)
        .stringMap(AsCollections.of("key1", "val1", "key2", "val2", "key3", "val3"))
        .build();
    protected static final IndexedPerson emilien = IndexedPerson.builder()
        .id(nextId())
        .firstName("Emilien")
        .lastName("Coutant-Kerbalec")
        .age(30)
        .intMap(AsCollections.of("key1", 0, "key2", 1))
        .ints(Arrays.asList(450, 550, 990))
        .build();
    protected static final List<IndexedPerson> allIndexedPersons = Arrays.asList(alain, luc, lilly, daniel, petra,
        emilien);
    @Autowired
    protected ReactiveIndexedPersonRepository reactiveRepository;
    @Autowired
    protected ReactiveBlockingAerospikeTestOperations reactiveBlockingAerospikeTestOperations;

    @BeforeAll
    public void beforeAll() {
        reactiveBlockingAerospikeTestOperations.deleteAll(reactiveRepository, allIndexedPersons);
        reactiveBlockingAerospikeTestOperations.saveAll(reactiveRepository, allIndexedPersons);
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_first_name_index", "firstName",
            IndexType.STRING).block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_last_name_index", "lastName",
            IndexType.STRING).block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_age_index", "age", IndexType.NUMERIC).block();
        reactiveTemplate.createIndex(IndexedPerson.class, "indexed_person_strings_index", "strings",
            IndexType.STRING, IndexCollectionType.LIST).block();
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

}
