package org.springframework.data.aerospike.repository.query.blocking.indexed;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.aerospike.sample.IndexedPersonRepository;

import java.util.ArrayList;
import java.util.List;

import static com.aerospike.client.query.IndexCollectionType.LIST;
import static com.aerospike.client.query.IndexCollectionType.MAPKEYS;
import static com.aerospike.client.query.IndexCollectionType.MAPVALUES;
import static com.aerospike.client.query.IndexType.NUMERIC;
import static com.aerospike.client.query.IndexType.STRING;
import static org.springframework.data.aerospike.util.AsCollections.of;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IndexedPersonRepositoryQueryTests extends BaseBlockingIntegrationTests {

    @BeforeEach
    public void beforeEach(TestInfo testInfo) {
        assertBinsAreIndexed(testInfo);
    }

    @Autowired
    protected IndexedPersonRepository repository;
    protected static final IndexedPerson john = IndexedPerson.builder()
        .id(nextId())
        .firstName("John")
        .lastName("Farmer")
        .age(42)
        .strings(List.of("str1", "str2"))
        .ints(List.of(450, 550, 990))
        .address(new Address("Foo Street 1", 1, "C0123", "Bar"))
        .build();
    protected static final IndexedPerson peter = IndexedPerson.builder()
        .id(nextId())
        .firstName("Peter")
        .lastName("Macintosh")
        .age(41)
        .strings(List.of("str1", "str2", "str3"))
        .build();
    protected static final IndexedPerson jane = IndexedPerson.builder()
        .id(nextId())
        .firstName("Jane")
        .lastName("Gillaham")
        .age(49)
        .intMap(of("key1", 0, "key2", 1))
        .ints(List.of(550, 600, 990))
        .address(new Address("Foo Street 2", 2, "C0124", "C0123"))
        .build();
    protected static final IndexedPerson billy = IndexedPerson.builder()
        .id(nextId())
        .firstName("Billy")
        .lastName("Smith")
        .age(25)
        .stringMap(of("key1", "val1", "key2", "val2"))
        .address(new Address(null, null, null, null))
        .build();
    protected static final IndexedPerson tricia = IndexedPerson.builder()
        .id(nextId())
        .firstName("Tricia")
        .lastName("James")
        .age(31)
        .intMap(of("key1", 0, "key2", 1))
        .build();
    protected static final List<IndexedPerson> allIndexedPersons = List.of(john, peter, jane, billy, tricia);

    @BeforeAll
    public void beforeAll() {
        additionalAerospikeTestOperations.deleteAll(repository, allIndexedPersons);
        additionalAerospikeTestOperations.saveAll(repository, allIndexedPersons);
        String setName = template.getSetName(IndexedPerson.class);

        List<Index> newIndexes = new ArrayList<>();
        newIndexes.add(Index.builder()
            .set(setName)
            .name("indexed_person_first_name_index")
            .bin("firstName")
            .indexType(STRING)
            .build());
        newIndexes.add(Index.builder()
            .set(setName)
            .name("indexed_person_last_name_index")
            .bin("lastName")
            .indexType(STRING)
            .build());
        newIndexes.add(Index.builder()
            .set(setName)
            .name("indexed_person_age_index")
            .bin("age")
            .indexType(NUMERIC)
            .build());
        newIndexes.add(Index.builder()
            .set(setName)
            .name("indexed_person_isActive_index")
            .bin("isActive")
            .indexType(STRING)
            .build());
        newIndexes.add(Index.builder()
            .set(setName)
            .name("indexed_person_strings_index")
            .bin("strings")
            .indexType(STRING)
            .indexCollectionType(LIST)
            .build());
        newIndexes.add(Index.builder()
            .set(setName)
            .name("indexed_person_ints_index")
            .bin("ints")
            .indexType(NUMERIC)
            .indexCollectionType(LIST)
            .build());
        newIndexes.add(Index.builder()
            .set(setName)
            .name("indexed_person_string_map_keys_index")
            .bin("stringMap")
            .indexType(STRING)
            .indexCollectionType(MAPKEYS)
            .build());
        newIndexes.add(Index.builder()
            .set(setName)
            .name("indexed_person_string_map_values_index")
            .bin("stringMap")
            .indexType(STRING)
            .indexCollectionType(MAPVALUES).build());
        newIndexes.add(Index.builder()
            .set(setName)
            .name("indexed_person_int_map_keys_index")
            .bin("intMap")
            .indexType(STRING)
            .indexCollectionType(MAPKEYS)
            .build());
        newIndexes.add(Index.builder()
            .set(setName)
            .name("indexed_person_int_map_values_index")
            .bin("intMap")
            .indexType(NUMERIC)
            .indexCollectionType(MAPVALUES)
            .build());
        newIndexes.add(Index.builder()
            .set(setName)
            .name("indexed_person_address_values_index")
            .bin("address")
            .indexType(STRING)
            .indexCollectionType(MAPVALUES)
            .build());
        newIndexes.add(Index.builder()
            .set(setName)
            .name("indexed_person_friend_address_values_index")
            .bin("friend")
            .indexType(STRING)
            .indexCollectionType(MAPVALUES)
            .ctx(new CTX[]{CTX.mapKey(Value.get("address"))})
            .build());
        newIndexes.add(Index.builder()
            .set(setName)
            .name("indexed_person_addressesList_0_values_index")
            .bin("addressesList")
            .indexType(STRING)
            .indexCollectionType(MAPVALUES)
            .ctx(new CTX[]{CTX.listIndex(0)})
            .build());
        newIndexes.add(Index.builder()
            .set(setName)
            .name("indexed_person_friend_bestFriend_address_values_index")
            .bin("friend")
            .indexType(STRING)
            .indexCollectionType(MAPVALUES)
            .ctx(new CTX[]{CTX.mapKey(Value.get("bestFriend")), CTX.mapKey(Value.get("address"))})
            .build());
        newIndexes.add(Index.builder()
            .set(setName)
            .name("indexed_person_friend_bestFriend_address_values_index_num")
            .bin("friend")
            .indexType(NUMERIC)
            .indexCollectionType(MAPVALUES)
            .ctx(new CTX[]{CTX.mapKey(Value.get("bestFriend")), CTX.mapKey(Value.get("address"))})
            .build());
        newIndexes.add(Index.builder()
            .set(setName)
            .name("indexed_person_bestFriend_friend_address_values_index")
            .bin("bestFriend")
            .indexType(STRING)
            .indexCollectionType(MAPVALUES)
            .ctx(new CTX[]{CTX.mapKey(Value.get("friend")), CTX.mapKey(Value.get("address"))})
            .build());
        additionalAerospikeTestOperations.createIndexes(newIndexes);
    }

    @AfterAll
    public void afterAll() {
        additionalAerospikeTestOperations.deleteAll(repository, allIndexedPersons);
        List<Index> dropIndexes = new ArrayList<>();
        String setName = template.getSetName(IndexedPerson.class);

        dropIndexes.add(Index.builder().set(setName).name("indexed_person_first_name_index").build());
        dropIndexes.add(Index.builder().set(setName).name("indexed_person_last_name_index").build());
        dropIndexes.add(Index.builder().set(setName).name("indexed_person_age_index").build());
        dropIndexes.add(Index.builder().set(setName).name("indexed_person_isActive_index").build());
        dropIndexes.add(Index.builder().set(setName).name("indexed_person_strings_index").build());
        dropIndexes.add(Index.builder().set(setName).name("indexed_person_ints_index").build());
        dropIndexes.add(Index.builder().set(setName).name("indexed_person_string_map_keys_index").build());
        dropIndexes.add(Index.builder().set(setName).name("indexed_person_string_map_values_index").build());
        dropIndexes.add(Index.builder().set(setName).name("indexed_person_int_map_keys_index").build());
        dropIndexes.add(Index.builder().set(setName).name("indexed_person_int_map_values_index").build());
        dropIndexes.add(Index.builder().set(setName).name("indexed_person_address_keys_index").build());
        dropIndexes.add(Index.builder().set(setName).name("indexed_person_address_values_index").build());
        dropIndexes.add(Index.builder().set(setName).name("indexed_person_friend_address_keys_index").build());
        dropIndexes.add(Index.builder().set(setName).name("indexed_person_friend_address_values_index").build());
        dropIndexes.add(Index.builder().set(setName).name("indexed_person_friend_bestFriend_address_keys_index")
            .build());
        additionalAerospikeTestOperations.dropIndexes(dropIndexes);
    }

    @AfterEach
    public void assertNoScans() {
        additionalAerospikeTestOperations.assertNoScansForSet(template.getSetName(IndexedPerson.class));
    }
}
