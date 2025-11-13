package org.springframework.data.aerospike.repository.query.blocking.indexed;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.annotation.Extensive;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.aerospike.sample.IndexedPersonRepository;
import org.springframework.data.aerospike.sample.Person;
import org.testcontainers.DockerClientFactory;

import java.util.List;

import static org.testcontainers.shaded.com.google.common.collect.ImmutableMap.of;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Slf4j
@Extensive
public abstract class IndexedPersonRepositoryQueryTests extends BaseBlockingIntegrationTests {

    @Autowired
    protected IndexedPersonRepository repository;
    protected final IndexedPerson john = IndexedPerson.builder()
            .id("1")
            .firstName("John")
            .lastName("Farmer")
            .age(42)
            .gender(Person.Gender.MALE)
            .strings(List.of("str1", "str2"))
            .ints(List.of(450, 550, 990))
            .address(new Address("Foo Street 1", 1, "C0123", "Bar"))
            .build();
    protected final IndexedPerson peter = IndexedPerson.builder()
            .id("2")
            .firstName("Peter")
            .lastName("Macintosh")
            .age(41)
            .gender(Person.Gender.MALE)
            .strings(List.of("str1", "str2", "str3"))
            .build();
    protected final IndexedPerson jane = IndexedPerson.builder()
            .id("3")
            .firstName("Jane")
            .lastName("Gillaham")
            .age(49)
            .gender(Person.Gender.FEMALE)
            .intMap(of("key1", 0, "key2", 1))
            .ints(List.of(550, 600, 990))
            .address(new Address("Foo Street 2", 2, "C0124", "C0123"))
            .build();
    protected final IndexedPerson billy = IndexedPerson.builder()
            .id("4")
            .firstName("Billy")
            .lastName("Smith")
            .age(25)
            .gender(Person.Gender.MALE)
            .stringMap(of("key1", "val1", "key2", "val2"))
            .address(new Address(null, null, null, null))
            .build();
    protected final IndexedPerson tricia = IndexedPerson.builder()
            .id("5")
            .firstName("Tricia")
            .lastName("James")
            .age(31)
            .gender(Person.Gender.FEMALE)
            .intMap(of("key1", 0, "key2", 1))
            .build();
    protected final List<IndexedPerson> allIndexedPersons = List.of(john, peter, jane, billy, tricia);

    protected abstract List<Index> newIndexes();

    @BeforeAll
    public void beforeAll() {
        //  additionalAerospikeTestOperations.deleteAll(repository, allIndexedPersons);
        // Run this once at startup or before your first container starts:
        DockerClientFactory.instance().client().pingCmd().exec();

        additionalAerospikeTestOperations.saveAll(repository, allIndexedPersons);

        try {
            additionalAerospikeTestOperations.createIndexes(newIndexes());
        } catch (Exception e) {
            log.info("Creating indexes failed due to: {}", e.getMessage());
        }
    }

    @AfterEach
    public void assertNoScans() {
        additionalAerospikeTestOperations.assertNoScansForSet(template.getSetName(IndexedPerson.class));
    }

    @AfterAll
    public void afterAll() {
        try {
            additionalAerospikeTestOperations.dropIndexes(newIndexes());
        } catch (Exception e) {
            log.info("Dropping indexes failed due to: {}", e.getMessage());
        }
    }
}
