package org.springframework.data.aerospike.repository;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.aerospike.sample.IndexedPersonRepository;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.utility.TestUtils;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.AsCollections.of;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IndexedPersonRepositoryQueryTests extends BaseBlockingIntegrationTests {

    @Autowired
    IndexedPersonRepository repository;
    static final IndexedPerson john = IndexedPerson.builder().id(nextId()).firstName("John").lastName("Farmer").age(42)
        .strings(List.of("str1", "str2")).ints(List.of(450, 550, 990))
        .address(new Address("Foo Street 1", 1, "C0123", "Bar")).build();
    static final IndexedPerson peter = IndexedPerson.builder().id(nextId()).firstName("Peter").lastName("Macintosh")
        .age(41)
        .strings(List.of("str1", "str2", "str3")).build();
    static final IndexedPerson jane = IndexedPerson.builder().id(nextId()).firstName("Jane").lastName("Gillaham")
        .age(49)
        .intMap(of("key1", 0, "key2", 1)).ints(List.of(550, 600, 990))
        .address(new Address("Foo Street 2", 2, "C0124", "C0123")).build();
    static final IndexedPerson billy = IndexedPerson.builder().id(nextId()).firstName("Billy").lastName("Smith").age(25)
        .stringMap(of("key1", "val1", "key2", "val2")).address(new Address(null, null, null, null))
        .build();
    static final IndexedPerson tricia = IndexedPerson.builder().id(nextId()).firstName("Tricia").lastName("James")
        .age(31)
        .intMap(of("key1", 0, "key2", 1)).build();
    public static final List<IndexedPerson> allIndexedPersons = List.of(john, peter, jane, billy, tricia);

    @AfterAll
    public void afterAll() {
        repository.deleteAll(allIndexedPersons);
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
        additionalAerospikeTestOperations.dropIndexIfExists(IndexedPerson.class,
            "indexed_person_friend_address_keys_index");
        additionalAerospikeTestOperations.dropIndexIfExists(IndexedPerson.class,
            "indexed_person_friend_address_values_index");
        additionalAerospikeTestOperations.dropIndexIfExists(IndexedPerson.class,
            "indexed_person_friend_bestFriend_address_keys_index");
        indexRefresher.refreshIndexes();
    }

    @BeforeAll
    public void beforeAll() {
        repository.deleteAll(allIndexedPersons);
        repository.saveAll(allIndexedPersons);
        additionalAerospikeTestOperations.createIndexIfNotExists(IndexedPerson.class, "indexed_person_last_name_index",
            "lastName", IndexType.STRING);
        additionalAerospikeTestOperations.createIndexIfNotExists(IndexedPerson.class,
            "indexed_person_first_name_index", "firstName", IndexType.STRING);
        additionalAerospikeTestOperations.createIndexIfNotExists(IndexedPerson.class, "indexed_person_age_index",
            "age", IndexType.NUMERIC);
        additionalAerospikeTestOperations.createIndexIfNotExists(IndexedPerson.class, "indexed_person_strings_index",
            "strings", IndexType.STRING, IndexCollectionType.LIST);
        additionalAerospikeTestOperations.createIndexIfNotExists(IndexedPerson.class, "indexed_person_ints_index",
            "ints", IndexType.NUMERIC, IndexCollectionType.LIST);
        additionalAerospikeTestOperations.createIndexIfNotExists(IndexedPerson.class,
            "indexed_person_string_map_keys_index", "stringMap", IndexType.STRING, IndexCollectionType.MAPKEYS);
        additionalAerospikeTestOperations.createIndexIfNotExists(IndexedPerson.class,
            "indexed_person_string_map_values_index", "stringMap", IndexType.STRING, IndexCollectionType.MAPVALUES);
        additionalAerospikeTestOperations.createIndexIfNotExists(IndexedPerson.class,
            "indexed_person_int_map_keys_index", "intMap", IndexType.STRING, IndexCollectionType.MAPKEYS);
        additionalAerospikeTestOperations.createIndexIfNotExists(IndexedPerson.class,
            "indexed_person_int_map_values_index", "intMap", IndexType.NUMERIC, IndexCollectionType.MAPVALUES);
        additionalAerospikeTestOperations.createIndexIfNotExists(IndexedPerson.class,
            "indexed_person_address_keys_index", "address", IndexType.STRING, IndexCollectionType.MAPKEYS);
        additionalAerospikeTestOperations.createIndexIfNotExists(IndexedPerson.class,
            "indexed_person_address_values_index", "address", IndexType.STRING, IndexCollectionType.MAPVALUES);
        additionalAerospikeTestOperations.createIndexIfNotExists(IndexedPerson.class,
            "indexed_person_friend_address_keys_index", "friend", IndexType.STRING,
            IndexCollectionType.MAPKEYS, CTX.mapKey(Value.get("address")));
        additionalAerospikeTestOperations.createIndexIfNotExists(IndexedPerson.class,
            "indexed_person_friend_address_values_index", "friend", IndexType.STRING,
            IndexCollectionType.MAPVALUES, CTX.mapValue(Value.get("address")));
        additionalAerospikeTestOperations.createIndexIfNotExists(IndexedPerson.class,
            "indexed_person_friend_bestFriend_address_keys_index", "friend", IndexType.STRING,
            IndexCollectionType.MAPKEYS, CTX.mapKey(Value.get("bestFriend")), CTX.mapKey(Value.get("address")));
        indexRefresher.refreshIndexes();
    }

    @AfterEach
    public void assertNoScans() {
        additionalAerospikeTestOperations.assertNoScansForSet(template.getSetName(IndexedPerson.class));
    }

    @Test
    void findByListContainingString_forExistingResult() {
        assertThat(repository.findByStringsContaining("str1")).containsOnly(john, peter);
        assertThat(repository.findByStringsContaining("str2")).containsOnly(john, peter);
        assertThat(repository.findByStringsContaining("str3")).containsOnly(peter);
    }

    @Test
    void findByListContainingString_forEmptyResult() {
        List<IndexedPerson> persons = repository.findByStringsContaining("str5");

        assertThat(persons).isEmpty();
    }

    @Test
    void findByListContainingInteger_forExistingResult() {
        assertThat(repository.findByIntsContaining(550)).containsOnly(john, jane);
        assertThat(repository.findByIntsContaining(990)).containsOnly(john, jane);
        assertThat(repository.findByIntsContaining(600)).containsOnly(jane);
    }

    @Test
    void findByListContainingInteger_forEmptyResult() {
        List<IndexedPerson> persons = repository.findByIntsContaining(7777);

        assertThat(persons).isEmpty();
    }


    @Test
    void findByListValueGreaterThan() {
        List<IndexedPerson> persons = repository.findByIntsGreaterThan(549);

        assertThat(persons).containsExactlyInAnyOrder(jane, john);
    }

    @Test
    void findByListValueLessThanOrEqual() {
        List<IndexedPerson> persons = repository.findByIntsLessThanEqual(500);

        assertThat(persons).containsOnly(john);
    }

    @Test
    void findByListValueInRange() {
        List<IndexedPerson> persons = repository.findByIntsBetween(500, 600);

        assertThat(persons).containsExactlyInAnyOrder(jane, john);
    }

    @Test
    public void findsPersonById() {
        Optional<IndexedPerson> person = repository.findById(john.getId());

        assertThat(person).hasValueSatisfying(actual -> {
            assertThat(actual).isInstanceOf(Person.class);
            assertThat(actual).isEqualTo(john);
        });
    }

    @Test
    public void findsAllWithGivenIds() {
        List<IndexedPerson> result = (List<IndexedPerson>) repository.findAllById(List.of(john.getId(),
            billy.getId()));

        assertThat(result)
            .contains(john, billy)
            .hasSize(2)
            .doesNotContain(jane, peter, tricia);
    }

    @Test
    public void findsPersonsByLastName() {
        List<IndexedPerson> result = repository.findByLastName("Gillaham");

        assertThat(result)
            .containsOnly(jane)
            .hasSize(1);
    }

    @Test
    public void findsPersonsByFirstName() {
        List<IndexedPerson> result = repository.findByFirstName("Tricia");

        assertThat(result)
            .hasSize(1)
            .containsOnly(tricia);
    }

    @Test
    public void findsPersonsByActiveAndFirstName() {
        assertThat(tricia.isActive()).isFalse();
        List<IndexedPerson> result = repository.findPersonsByActiveAndFirstName(false, "Tricia");

        assertThat(result)
            .hasSize(1)
            .containsOnly(tricia);

    }

    @Test
    public void countByLastName_forExistingResult() {
        assertThatThrownBy(() -> repository.countByLastName("Lerois"))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Query method IndexedPerson.countByLastName not supported.");

//		assertThat(result).isEqualTo(2);
    }

    @Test
    public void countByLastName_forEmptyResult() {
        assertThatThrownBy(() -> repository.countByLastName("Smirnova"))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Query method IndexedPerson.countByLastName not supported.");

//		assertThat(result).isEqualTo(0);
    }

    @Test
    public void findByAgeGreaterThan_forExistingResult() {
        Slice<IndexedPerson> slice = repository.findByAgeGreaterThan(40, PageRequest.of(0, 10));

        assertThat(slice.hasContent()).isTrue();
        assertThat(slice.hasNext()).isFalse();
        assertThat(slice.getContent()).hasSize(3).contains(john, jane, peter);
    }

    @Test
    public void findByAgeGreaterThan_respectsLimit() {
        Slice<IndexedPerson> slice = repository.findByAgeGreaterThan(40, PageRequest.of(0, 3));

        assertThat(slice.hasContent()).isTrue();
        assertThat(slice.getContent()).contains(john, jane, peter);
    }

    @Test
    public void findByAgeGreaterThan_respectsLimitAndOffsetAndSort() {
        List<IndexedPerson> result = IntStream.range(0, 4)
            .mapToObj(index -> repository.findByAgeGreaterThan(40, PageRequest.of(index, 1, Sort.by("age"))))
            .flatMap(slice -> slice.getContent().stream())
            .collect(Collectors.toList());

        assertThat(result)
            .hasSize(3)
            .containsSequence(peter, john, jane);
    }

    @Test
    public void findByAgeGreaterThan_returnsValidValuesForNextAndPrev() {
        Slice<IndexedPerson> first = repository.findByAgeGreaterThan(40, PageRequest.of(0, 1, Sort.by("age")));

        assertThat(first.hasContent()).isTrue();
        assertThat(first.getNumberOfElements()).isEqualTo(1);
        assertThat(first.hasNext()).isFalse(); // TODO: not implemented yet. should be true instead
        assertThat(first.isFirst()).isTrue();

        Slice<IndexedPerson> last = repository.findByAgeGreaterThan(40, PageRequest.of(2, 1, Sort.by("age")));

        assertThat(last.hasContent()).isTrue();
        assertThat(last.getNumberOfElements()).isEqualTo(1);
        assertThat(last.hasNext()).isFalse();
        assertThat(last.isLast()).isTrue();
    }

    @Test
    public void findByAgeGreaterThan_forEmptyResult() {
        Slice<IndexedPerson> slice = repository.findByAgeGreaterThan(100, PageRequest.of(0, 10));

        assertThat(slice.hasContent()).isFalse();
        assertThat(slice.hasNext()).isFalse();
        assertThat(slice.getContent()).isEmpty();
    }

    @Test
    public void findsPersonsByFirstNameAndByAge() {
        List<IndexedPerson> result = repository.findByFirstNameAndAge("Billy", 25);
        assertThat(result).containsOnly(billy);

        result = repository.findByFirstNameAndAge("Peter", 41);
        assertThat(result).containsOnly(peter);
    }

    @Test
    public void findsPersonInAgeRangeCorrectly() {
        Iterable<IndexedPerson> it = repository.findByAgeBetween(40, 45);

        assertThat(it).hasSize(2).contains(john, peter);
    }

    @Test
    public void findsPersonInAgeRangeCorrectlyOrderByLastName() {
        Iterable<IndexedPerson> it = repository.findByAgeBetweenOrderByLastName(30, 45);

        assertThat(it).hasSize(3);
    }

    @Test
    public void findsPersonInAgeRangeAndNameCorrectly() {
        Iterable<IndexedPerson> it = repository.findByAgeBetweenAndLastName(40, 45, "Matthews");
        assertThat(it).hasSize(0);

        Iterable<IndexedPerson> result = repository.findByAgeBetweenAndLastName(20, 26, "Smith");
        assertThat(result).hasSize(1).contains(billy);
    }

    @Test
    public void findsPersonInAgeRangeOrNameCorrectly() {
        Iterable<IndexedPerson> it = repository.findByAgeBetweenOrLastName(40, 45, "James");
        assertThat(it).containsExactlyInAnyOrder(john, peter, tricia);

        Iterable<IndexedPerson> result = repository.findByAgeBetweenOrLastName(20, 26, "Macintosh");
        assertThat(result).containsExactlyInAnyOrder(billy, peter);
    }

    @Test
    void findByMapKeysContaining() {
        assertThat(billy.getStringMap()).containsKey("key1");

        List<IndexedPerson> persons = repository.findByStringMapContaining("key1",
            CriteriaDefinition.AerospikeMapCriteria.KEY);
        assertThat(persons).contains(billy);
    }

    @Test
    void findByMapValuesContaining() {
        assertThat(billy.getStringMap()).containsValue("val1");

        List<IndexedPerson> persons = repository.findByStringMapContaining("val1",
            CriteriaDefinition.AerospikeMapCriteria.VALUE);
        assertThat(persons).contains(billy);
    }

    @Test
    void findByMapKeyValueEqualsInt() {
        assertThat(tricia.getIntMap()).containsKey("key1");
        assertThat(tricia.getIntMap()).containsValue(0);

        Iterable<IndexedPerson> result = repository.findByIntMapEquals("key1", 0);
        assertThat(result).contains(tricia);
    }

    @Test
    void findByMapKeyValueEqualsString() {
        assertThat(billy.getStringMap()).containsKey("key1");
        assertThat(billy.getStringMap()).containsValue("val1");

        List<IndexedPerson> persons = repository.findByStringMapEquals("key1", "val1");
        assertThat(persons).contains(billy);
    }

    @Test
    void findPersonsByAddressZipCode() {
        assertThat(john.getAddress().getZipCode()).isEqualTo("C0123");
        List<IndexedPerson> result = repository.findByAddressZipCode("C0123");
        assertThat(result).contains(john);
    }

    @Test
    void findPersonsByFriendAddressZipCode() {
        assertThat(john.getAddress().getZipCode()).isEqualTo("C0123");
        jane.setFriend(john);
        repository.save(jane);

        List<IndexedPerson> result = repository.findByFriendAddressZipCode("C0123");
        assertThat(result).contains(jane);
        TestUtils.setFriendsToNull(repository, jane);
    }

    @Test
    void findPersonsByFriendBestFriendAddressZipCode() {
        assertThat(john.getAddress().getZipCode()).isEqualTo("C0123");
        jane.setBestFriend(john);
        repository.save(jane);
        peter.setFriend(jane);
        repository.save(peter);

        List<IndexedPerson> result = repository.findByFriendBestFriendAddressZipCode("C0123");
        assertThat(result).contains(peter);
        TestUtils.setFriendsToNull(repository, jane, peter);
    }

    @Test
    void findPersonsByFriendBestFriendAddressApartment() {
        assertThat(john.getAddress().getApartment()).isEqualTo(1);
        jane.setBestFriend(john);
        repository.save(jane);
        peter.setFriend(jane);
        repository.save(peter);

        List<IndexedPerson> result = repository.findByFriendBestFriendAddressApartment(1);
        assertThat(result).contains(peter);
        TestUtils.setFriendsToNull(repository, jane, peter);
    }

    @Test
    void findByMapKeyValueGreaterThan() {
        assertThat(tricia.getIntMap()).containsKey("key2");
        assertThat(tricia.getIntMap().get("key2") > 0).isTrue();

        List<IndexedPerson> persons = repository.findByIntMapGreaterThan("key2", 0);
        assertThat(persons).contains(tricia);
    }

    @Test
    void findByMapKeyValueLessThanOrEqual() {
        assertThat(tricia.getIntMap()).containsKey("key2");
        assertThat(tricia.getIntMap().get("key2") > 0).isTrue();

        List<IndexedPerson> persons = repository.findByIntMapLessThanEqual("key2", 1);
        assertThat(persons).containsExactlyInAnyOrder(tricia, jane);
    }

    @Test
    void findByMapKeyValueBetween() {
        assertThat(jane.getIntMap()).containsKey("key2");
        assertThat(tricia.getIntMap()).containsKey("key2");
        assertThat(jane.getIntMap().get("key2") >= 0).isTrue();
        assertThat(tricia.getIntMap().get("key2") >= 0).isTrue();

        List<IndexedPerson> persons = repository.findByIntMapBetween("key2", 0, 1);
        assertThat(persons).containsExactlyInAnyOrder(tricia, jane);
    }

    @Test
    void findByBestFriendFriendIntMapKeyValueBetween() {
        assertThat(tricia.getIntMap()).containsKey("key2");
        assertThat(tricia.getIntMap().get("key2") >= 0).isTrue();

        jane.setFriend(tricia);
        repository.save(jane);
        peter.setBestFriend(jane);
        repository.save(peter);

        List<IndexedPerson> persons = repository.findByBestFriendFriendIntMapBetween("key2", 0, 1);
        assertThat(persons).contains(peter);

        TestUtils.setFriendsToNull(repository, jane, peter);
    }

    @Test
    void findByBestFriendFriendAddressApartmentBetween() {
        assertThat(jane.getAddress().getApartment()).isEqualTo(2);

        tricia.setFriend(jane);
        repository.save(tricia);
        billy.setBestFriend(tricia);
        repository.save(billy);

        List<IndexedPerson> persons = repository.findByBestFriendFriendAddressApartmentBetween(1, 2);
        assertThat(persons).contains(billy);

        TestUtils.setFriendsToNull(repository, tricia, billy);
    }
}
