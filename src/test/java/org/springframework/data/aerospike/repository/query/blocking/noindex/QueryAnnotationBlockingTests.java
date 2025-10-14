package org.springframework.data.aerospike.repository.query.blocking.noindex;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.PersonNegativeTestsRepository;
import org.springframework.data.aerospike.sample.PersonQueryAnnotationRepository;
import org.springframework.data.aerospike.util.AsCollections;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.sample.Person.Gender.FEMALE;

public class QueryAnnotationBlockingTests extends BaseBlockingIntegrationTests {

    @Autowired
    protected PersonQueryAnnotationRepository<Person> repository;
    @Autowired
    protected PersonNegativeTestsRepository<Person> negativeTestsRepository;

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
        .gender(FEMALE)
        .age(30)
        .ints(List.of(550, 600, 990))
        .build();
    protected static final List<Person> allPersons = List.of(dave, donny, oliver, alicia);

    @BeforeAll
    void beforeAll() {
        additionalAerospikeTestOperations.deleteAllAndVerify(Person.class);
        additionalAerospikeTestOperations.saveAll(repository, allPersons);
    }

    @AfterAll
    void afterAll() {
        additionalAerospikeTestOperations.deleteAll(repository, allPersons);
    }

    @Test
    void findByCollectionEquals_QueryAnnotation_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByStringsEquals(List.of("string1", "string2")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Element at index 0 is expected to be a String, number or boolean");
    }

    @Test
    // Repository method names are annotated with @Query using static DSL string
    void findBySimplePropertyEquals_String_static_DSL_expression() {
        assertThat(repository.findByLastName("Macintire")).containsOnly(donny);

        // DSL string is static for this query method (see the method definition), so the given parameter is not used
        assertThat(repository.findByLastName("TestTest")).containsOnly(donny);
    }

    @Test
    // Repository method names are annotated with @Query using DSL string with placeholders
    void findBySimplePropertyEquals_String_DSL_expression_with_placeholders() {
        assertThat(repository.findByFirstName("Dave")).containsOnly(dave);

        // DSL string is not static for this query method,
        // and the query method parameters are used just like in regular query methods
        assertThat(repository.findByFirstName("TestTest")).isEmpty();

        // "IgnoreCase" query modifier does not work in this case as query is governed by DSL string given to @Query
        assertThat(repository.findByFirstNameIgnoreCase("Dave")).containsOnly(dave);
        assertThat(repository.findByFirstNameIgnoreCase("dAvE")).isEmpty();
    }

    @Test
    // Repository method names are annotated with @Query using DSL string with placeholders
    void findBySimplePropertyGreaterThan_DSL_expression_with_placeholders() {
        assertThat(repository.findByAgeGreaterThan(30)).containsExactlyInAnyOrder(donny, dave);
    }

    @Test
    // Repository method names are annotated with @Query using DSL string with placeholders
    void findBySimpleBooleanProperty_DSL_expression_with_placeholders() {
        oliver.setActive(true);
        repository.save(oliver);
        alicia.setActive(true);
        repository.save(alicia);
        assertThat(repository.findByIsActive(true)).containsExactlyInAnyOrder(oliver, alicia);

        // Cleanup
        oliver.setActive(false);
        repository.save(oliver);
        alicia.setActive(false);
        repository.save(alicia);
    }
}
