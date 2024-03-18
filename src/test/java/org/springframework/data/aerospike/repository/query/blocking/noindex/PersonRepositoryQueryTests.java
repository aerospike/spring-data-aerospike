package org.springframework.data.aerospike.repository.query.blocking.noindex;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.AsCollections;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.PersonNegativeTestsRepository;
import org.springframework.data.aerospike.sample.PersonRepository;

import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PersonRepositoryQueryTests extends BaseBlockingIntegrationTests {

    protected static final Person dave = Person.builder()
        .id(nextId())
        .firstName("Dave")
        .lastName("Matthews")
        .age(42)
        .strings(List.of("str0", "str1", "str2"))
        .address(new Address("Foo Street 1", 1, "C0123", "Bar"))
        .build();
    protected static final Person donny = Person.builder()
        .id(nextId())
        .firstName("Donny")
        .lastName("Macintire")
        .age(39)
        .strings(List.of("str1", "str2", "str3"))
        .stringMap(AsCollections.of("key1", "val1"))
        .build();
    protected static final Person oliver = Person.builder()
        .id(nextId())
        .firstName("Oliver August")
        .lastName("Matthews")
        .age(14)
        .ints(List.of(425, 550, 990))
        .build();
    protected static final Person alicia = Person.builder()
        .id(nextId())
        .firstName("Alicia")
        .lastName("Keys")
        .age(30)
        .ints(List.of(550, 600, 990))
        .build();
    protected static final Person carter = Person.builder()
        .id(nextId())
        .firstName("Carter")
        .lastName("Beauford")
        .age(49)
        .intMap(AsCollections.of("key1", 0, "key2", 1))
        .address(new Address("Foo Street 2", 2, "C0124", "C0123"))
        .build();
    protected static final Person boyd = Person.builder()
        .id(nextId())
        .firstName("Boyd")
        .lastName("Tinsley")
        .age(45)
        .stringMap(AsCollections.of("key1", "val1", "key2", "val2"))
        .address(new Address(null, null, null, null))
        .build();
    protected static final Person stefan = Person.builder()
        .id(nextId())
        .firstName("Stefan")
        .lastName("Lessard")
        .age(34)
        .build();
    protected static final Person leroi = Person.builder()
        .id(nextId())
        .firstName("Leroi")
        .lastName("Moore")
        .age(44)
        .intArray(new int[]{5, 6, 7, 8, 9, 10})
        .build();
    protected static final Person leroi2 = Person.builder()
        .id(nextId())
        .firstName("Leroi")
        .lastName("Moore")
        .age(25)
        .build();
    protected static final Person matias = Person.builder()
        .id(nextId())
        .firstName("Matias")
        .lastName("Craft")
        .age(24)
        .intArray(new int[]{1, 2, 3, 4, 5})
        .build();
    protected static final Person douglas = Person.builder()
        .id(nextId())
        .firstName("Douglas")
        .lastName("Ford")
        .age(25)
        .build();
    protected static final List<Person> allPersons = List.of(dave, donny, oliver, alicia, carter, boyd, stefan,
        leroi, leroi2, matias, douglas);

    @Autowired
    protected PersonRepository<Person> repository;
    @Autowired
    protected PersonNegativeTestsRepository<Person> negativeTestsRepository;

    @BeforeAll
    void beforeAll() {
        additionalAerospikeTestOperations.deleteAllAndVerify(Person.class);
        additionalAerospikeTestOperations.saveAll(repository, allPersons);
    }

    @AfterAll
    void afterAll() {
        additionalAerospikeTestOperations.deleteAll(repository, allPersons);
    }
}
