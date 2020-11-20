package org.springframework.data.aerospike.repository;

import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.repository.PersonTestData.Indexed;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.aerospike.sample.IndexedPersonRepository;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.repository.PersonTestData.Indexed.alicia;
import static org.springframework.data.aerospike.repository.PersonTestData.Indexed.boyd;
import static org.springframework.data.aerospike.repository.PersonTestData.Indexed.carter;
import static org.springframework.data.aerospike.repository.PersonTestData.Indexed.dave;
import static org.springframework.data.aerospike.repository.PersonTestData.Indexed.donny;
import static org.springframework.data.aerospike.repository.PersonTestData.Indexed.leroi;
import static org.springframework.data.aerospike.repository.PersonTestData.Indexed.leroi2;
import static org.springframework.data.aerospike.repository.PersonTestData.Indexed.oliver;
import static org.springframework.data.aerospike.repository.PersonTestData.Indexed.stefan;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IndexedPersonRepositoryQueryTests extends BaseBlockingIntegrationTests {

    @Autowired
    IndexedPersonRepository repository;

    @AfterAll
    public void afterAll() {
        additionalAerospikeTestOperations.deleteAll(IndexedPerson.class);
    }

    @BeforeAll
    public void beforeAll() {
        additionalAerospikeTestOperations.deleteAll(IndexedPerson.class);

        repository.saveAll(Indexed.all);

        additionalAerospikeTestOperations.createIndexIfNotExists(IndexedPerson.class, "indexed_person_last_name_index", "lastName", IndexType.STRING);
        additionalAerospikeTestOperations.createIndexIfNotExists(IndexedPerson.class, "indexed_person_first_name_index", "firstName", IndexType.STRING);
        additionalAerospikeTestOperations.createIndexIfNotExists(IndexedPerson.class, "indexed_person_age_index", "age", IndexType.NUMERIC);
        additionalAerospikeTestOperations.createIndexIfNotExists(IndexedPerson.class, "indexed_person_strings_index", "strings", IndexType.STRING, IndexCollectionType.LIST);
        additionalAerospikeTestOperations.createIndexIfNotExists(IndexedPerson.class, "indexed_person_ints_index", "ints", IndexType.NUMERIC, IndexCollectionType.LIST);
        additionalAerospikeTestOperations.createIndexIfNotExists(IndexedPerson.class, "indexed_person_map_keys_index", "map", IndexType.STRING, IndexCollectionType.MAPKEYS);
        additionalAerospikeTestOperations.createIndexIfNotExists(IndexedPerson.class, "indexed_person_map_values_index", "map", IndexType.STRING, IndexCollectionType.MAPVALUES);
        additionalAerospikeTestOperations.createIndexIfNotExists(IndexedPerson.class, "indexed_person_intmap_values_index", "intMap", IndexType.NUMERIC, IndexCollectionType.MAPKEYS);
        additionalAerospikeTestOperations.createIndexIfNotExists(IndexedPerson.class, "indexed_person_intmap_values_index", "intMap", IndexType.NUMERIC, IndexCollectionType.MAPVALUES);
        indexRefresher.refreshIndexes();
    }

    @AfterEach
    public void assertNoScans() {
        additionalAerospikeTestOperations.assertNoScansForSet(template.getSetName(IndexedPerson.class));
    }

    @Test
    void findByListContainingString_forExistingResult() {
        assertThat(repository.findByStringsContaining("str1")).containsOnly(dave, donny);
        assertThat(repository.findByStringsContaining("str2")).containsOnly(dave, donny);
        assertThat(repository.findByStringsContaining("str3")).containsOnly(donny);
    }

    @Test
    void findByListContainingString_forEmptyResult() {
        List<IndexedPerson> persons = repository.findByStringsContaining("str5");

        assertThat(persons).isEmpty();
    }

    @Test
    void findByListContainingInteger_forExistingResult() {
        assertThat(repository.findByIntsContaining(550)).containsOnly(leroi2, alicia);
        assertThat(repository.findByIntsContaining(990)).containsOnly(leroi2, alicia);
        assertThat(repository.findByIntsContaining(600)).containsOnly(alicia);
    }

    @Test
    void findByListContainingInteger_forEmptyResult() {
        List<IndexedPerson> persons = repository.findByIntsContaining(7777);

        assertThat(persons).isEmpty();
    }

    @Test
    public void findsPersonById() {
        Optional<IndexedPerson> person = repository.findById(dave.getId());

        assertThat(person).hasValueSatisfying(actual -> {
            assertThat(actual).isInstanceOf(Person.class);
            assertThat(actual).isEqualTo(dave);
        });
    }

    @Test
    public void findsAllWithGivenIds() {
        List<IndexedPerson> result = (List<IndexedPerson>) repository.findAllById(Arrays.asList(dave.getId(), boyd.getId()));

        assertThat(result)
                .contains(dave, boyd)
                .hasSize(2)
                .doesNotContain(oliver, carter, stefan, leroi, alicia);
    }

    @Test
    public void findsPersonsByLastname() {
        List<IndexedPerson> result = repository.findByLastName("Beauford");

        assertThat(result)
                .containsOnly(carter)
                .hasSize(1);
    }

    @Test
    public void findsPersonsByFirstname() {
        List<IndexedPerson> result = repository.findByFirstName("Leroi");

        assertThat(result)
                .containsOnly(leroi, leroi2)
                .hasSize(2);
    }

    @Test
    public void countByLastName_forExistingResult() {
        assertThatThrownBy(() -> repository.countByLastName("Leroi"))
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
        assertThat(slice.getContent()).hasSize(4).contains(dave, carter, boyd, leroi);
    }

    @Test
    public void findByAgeGreaterThan_respectsLimit() {
        Slice<IndexedPerson> slice = repository.findByAgeGreaterThan(40, PageRequest.of(0, 1));

        assertThat(slice.hasContent()).isTrue();
        assertThat(slice.hasNext()).isFalse();//TODO: not implemented yet. should be true instead
        assertThat(slice.getContent()).containsAnyOf(dave, carter, boyd, leroi).hasSize(1);
    }

    @Test
    public void findByAgeGreaterThan_respectsLimitAndOffsetAndSort() {
        List<IndexedPerson> result = IntStream.range(0, 4)
                .mapToObj(index -> repository.findByAgeGreaterThan(40, PageRequest.of(index, 1, Sort.by("age"))))
                .flatMap(slice -> slice.getContent().stream())
                .collect(Collectors.toList());

        assertThat(result)
                .hasSize(4)
                .containsSequence(leroi, dave, boyd, carter);
    }

    @Test
    public void findByAgeGreaterThan_returnsValidValuesForNextAndPrev() {
        Slice<IndexedPerson> first = repository.findByAgeGreaterThan(40, PageRequest.of(0, 1, Sort.by("age")));

        assertThat(first.hasContent()).isTrue();
        assertThat(first.getNumberOfElements()).isEqualTo(1);
        assertThat(first.hasNext()).isFalse();//TODO: not implemented yet. should be true instead
        assertThat(first.isFirst()).isTrue();

        Slice<IndexedPerson> last = repository.findByAgeGreaterThan(40, PageRequest.of(3, 1, Sort.by("age")));

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
    public void findsPersonsByFirstnameAndByAge() {
        List<IndexedPerson> result = repository.findByFirstNameAndAge("Leroi", 25);
        assertThat(result).containsOnly(leroi2);

        result = repository.findByFirstNameAndAge("Leroi", 41);
        assertThat(result).containsOnly(leroi);
    }

    @Test
    public void findsPersonInAgeRangeCorrectly() {
        Iterable<IndexedPerson> it = repository.findByAgeBetween(40, 45);

        assertThat(it).hasSize(3).contains(dave);
    }

    @Test
    public void findsPersonInAgeRangeCorrectlyOrderByLastName() {
        Iterable<IndexedPerson> it = repository.findByAgeBetweenOrderByLastName(30, 45);

        assertThat(it).hasSize(6);
    }

    @Test
    public void findsPersonInAgeRangeAndNameCorrectly() {
        Iterable<IndexedPerson> it = repository.findByAgeBetweenAndLastName(40, 45, "Matthews");
        assertThat(it).hasSize(1);

        Iterable<IndexedPerson> result = repository.findByAgeBetweenAndLastName(20, 26, "Moore");
        assertThat(result).hasSize(1);
    }

}
