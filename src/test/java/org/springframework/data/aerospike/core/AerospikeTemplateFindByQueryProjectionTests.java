package org.springframework.data.aerospike.core;

import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.PersonSomeFields;
import org.springframework.data.aerospike.utility.QueryUtils;
import org.springframework.data.aerospike.utility.ServerVersionUtils;
import org.springframework.data.domain.Sort;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AerospikeTemplateFindByQueryProjectionTests extends BaseBlockingIntegrationTests {

    final Person jean = Person.builder()
        .id(nextId()).firstName("Jean").lastName("Matthews").emailAddress("jean@gmail.com").age(21).build();
    final Person ashley = Person.builder()
        .id(nextId()).firstName("Ashley").lastName("Matthews").emailAddress("ashley@gmail.com").age(22).build();
    final Person beatrice = Person.builder()
        .id(nextId()).firstName("Beatrice").lastName("Matthews").emailAddress("beatrice@gmail.com").age(23).build();
    final Person dave = Person.builder()
        .id(nextId()).firstName("Dave").lastName("Matthews").emailAddress("dave@gmail.com").age(24).build();
    final Person zaipper = Person.builder()
        .id(nextId()).firstName("Zaipper").lastName("Matthews").emailAddress("zaipper@gmail.com").age(25).build();
    final Person knowlen = Person.builder()
        .id(nextId()).firstName("knowlen").lastName("Matthews").emailAddress("knowlen@gmail.com").age(26).build();
    final Person xylophone = Person.builder()
        .id(nextId()).firstName("Xylophone").lastName("Matthews").emailAddress("xylophone@gmail.com").age(27).build();
    final Person mitch = Person.builder()
        .id(nextId()).firstName("Mitch").lastName("Matthews").emailAddress("mitch@gmail.com").age(28).build();
    final Person alister = Person.builder()
        .id(nextId()).firstName("Alister").lastName("Matthews").emailAddress("alister@gmail.com").age(29).build();
    final Person aabbot = Person.builder()
        .id(nextId()).firstName("Aabbot").lastName("Matthews").emailAddress("aabbot@gmail.com").age(30).build();

    final List<Person> allPersons = Arrays.asList(jean, ashley, beatrice, dave, zaipper, knowlen, xylophone, mitch, alister,
        aabbot);

    @BeforeAll
    public void beforeAllSetUp() {
        deleteOneByOne(allPersons);

        // batch write operations are supported starting with Server version 6.0+
        if (ServerVersionUtils.isBatchWriteSupported(client)) {
            template.insertAll(allPersons);
            template.insertAll(allPersons, OVERRIDE_SET_NAME);
        } else {
            allPersons.forEach(person -> {
                template.insert(person);
                template.insert(person, OVERRIDE_SET_NAME);
            });
        }

        additionalAerospikeTestOperations.createIndex(Person.class, "person_age_index", "age",
            IndexType.NUMERIC);
        additionalAerospikeTestOperations.createIndex(Person.class, "person_first_name_index", "firstName"
            , IndexType.STRING);
        additionalAerospikeTestOperations.createIndex(Person.class, "person_last_name_index", "lastName",
            IndexType.STRING);
        additionalAerospikeTestOperations.createIndex(OVERRIDE_SET_NAME, "person_set_age_index", "age",
            IndexType.NUMERIC);
        additionalAerospikeTestOperations.createIndex(OVERRIDE_SET_NAME, "person_set_first_name_index", "firstName"
            , IndexType.STRING);
        additionalAerospikeTestOperations.createIndex(OVERRIDE_SET_NAME, "person_set_last_name_index", "lastName",
            IndexType.STRING);
    }

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
    }

    @AfterAll
    public void afterAll() {
        deleteOneByOne(allPersons);
        deleteOneByOne(allPersons, OVERRIDE_SET_NAME);
        additionalAerospikeTestOperations.dropIndex(Person.class, "person_age_index");
        additionalAerospikeTestOperations.dropIndex(Person.class, "person_first_name_index");
        additionalAerospikeTestOperations.dropIndex(Person.class, "person_last_name_index");
        additionalAerospikeTestOperations.dropIndex(OVERRIDE_SET_NAME, "person_set_age_index");
        additionalAerospikeTestOperations.dropIndex(OVERRIDE_SET_NAME, "person_set_first_name_index");
        additionalAerospikeTestOperations.dropIndex(OVERRIDE_SET_NAME, "person_set_last_name_index");
    }

    @Test
    public void findWithFilterEqualProjection() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByFirstName", "Dave");

        Stream<PersonSomeFields> result = template.find(query, Person.class, PersonSomeFields.class);

        assertThat(result).containsOnly(PersonSomeFields.builder()
            .firstName("Dave")
            .lastName("Matthews")
            .emailAddress("dave@gmail.com")
            .build());
    }

    @Test
    public void findWithFilterEqualProjectionWithSetName() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByFirstName", "Dave");

        Stream<PersonSomeFields> result = template.find(query, PersonSomeFields.class, OVERRIDE_SET_NAME);

        assertThat(result).containsOnly(PersonSomeFields.builder()
            .firstName("Dave")
            .lastName("Matthews")
            .emailAddress("dave@gmail.com")
            .build());
    }

    @Test
    public void findWithFilterEqualOrderByAscProjection() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByLastNameOrderByFirstNameAsc", "Matthews");

        Stream<PersonSomeFields> result = template.find(query, Person.class, PersonSomeFields.class);

        assertThat(result)
            .hasSize(10)
            .containsExactly(aabbot.toPersonSomeFields(), alister.toPersonSomeFields(), ashley.toPersonSomeFields(),
                beatrice.toPersonSomeFields(), dave.toPersonSomeFields(), jean.toPersonSomeFields(),
                knowlen.toPersonSomeFields(), mitch.toPersonSomeFields(), xylophone.toPersonSomeFields(),
                zaipper.toPersonSomeFields());
    }

    @Test
    public void findInRange_shouldFindLimitedNumberOfDocumentsProjection() {
        int skip = 0;
        int limit = 5;
        Stream<PersonSomeFields> stream = template.findInRange(skip, limit, Sort.unsorted(), Person.class,
            PersonSomeFields.class);

        assertThat(stream)
            .hasSize(5);
    }

    @Test
    public void findAll_findsAllExistingDocumentsProjection() {
        Stream<PersonSomeFields> result = template.findAll(Person.class, PersonSomeFields.class);

        assertThat(result).containsAll(allPersons.stream().map(Person::toPersonSomeFields).collect(Collectors.toList()));
    }

    @Test
    public void findAll_findsAllExistingDocumentsProjectionWithSetName() {
        Stream<PersonSomeFields> result = template.findAll(PersonSomeFields.class, OVERRIDE_SET_NAME);

        assertThat(result).containsAll(allPersons.stream().map(Person::toPersonSomeFields).collect(Collectors.toList()));
    }
}
