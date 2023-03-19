package org.springframework.data.aerospike.repository;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.IndexUtils;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.PersonRepository;
import org.springframework.data.aerospike.sample.PersonSomeFields;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.repository.PersonTestData.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PersonRepositoryQueryTests extends BaseBlockingIntegrationTests {

    @Autowired
    PersonRepository<Person> repository;

    @AfterAll
    public void afterAll() {
        additionalAerospikeTestOperations.deleteAllAndVerify(Person.class);
    }

    @BeforeAll
    public void beforeAll() {
        additionalAerospikeTestOperations.deleteAllAndVerify(Person.class);
        repository.saveAll(all);
    }

    @Test
    void findByListContainingString_forExistingResult() {
        assertThat(repository.findByStringsContaining("str1")).containsOnly(dave, donny);
        assertThat(repository.findByStringsContaining("str2")).containsOnly(dave, donny);
        assertThat(repository.findByStringsContaining("str3")).containsOnly(donny);
    }

    @Test
    void findByListContainingString_forEmptyResult() {
        List<Person> persons = repository.findByStringsContaining("str5");

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
        List<Person> persons = repository.findByIntsContaining(7777);

        assertThat(persons).isEmpty();
    }

    @Test
    void findByListValueGreaterThan() {
        List<Person> persons = repository.findByIntsGreaterThan(549);

        assertThat(persons).containsOnly(leroi2, alicia);
    }

    @Test
    void findByListValueLessThanOrEqual() {
        List<Person> persons = repository.findByIntsLessThanEqual(500);

        assertThat(persons).containsOnly(leroi2);
    }

    @Test
    void findByListValueInRange() {
        List<Person> persons = repository.findByIntsBetween(500, 600);

        assertThat(persons).containsExactlyInAnyOrder(leroi2, alicia);
    }

    @Test
    void findByMapKeysContaining() {
        assertThat(stefan.getStringMap()).containsKey("key1");
        assertThat(boyd.getStringMap()).containsKey("key1");

        List<Person> persons = repository.findByStringMapContaining("key1",
            CriteriaDefinition.AerospikeMapCriteria.KEY);

        assertThat(persons).contains(stefan, boyd);
    }

    @Test
    void findByMapValuesContaining() {
        assertThat(stefan.getStringMap()).containsValue("val1");
        assertThat(boyd.getStringMap()).containsValue("val1");

        List<Person> persons = repository.findByStringMapContaining("val1",
            CriteriaDefinition.AerospikeMapCriteria.VALUE);

        assertThat(persons).contains(stefan, boyd);
    }

    @Test
    void findByMapKeyValueEquals() {
        assertThat(stefan.getStringMap()).containsKey("key1");
        assertThat(stefan.getStringMap()).containsValue("val1");
        assertThat(boyd.getStringMap()).containsKey("key1");
        assertThat(boyd.getStringMap()).containsValue("val1");

        List<Person> persons = repository.findByStringMapEquals("key1", "val1");

        assertThat(persons).contains(stefan, boyd);
    }

    @Test
    void findByMapKeyValueNotEquals() {
        assertThat(leroi.getIntMap()).containsKey("key1");
        assertThat(!leroi.getIntMap().containsValue(22)).isTrue();

        List<Person> persons = repository.findByIntMapIsNot("key1", 22);

        assertThat(persons).contains(leroi);
    }

    @Test
    void findByMapKeyValueContains() {
        assertThat(stefan.getStringMap()).containsKey("key1");
        assertThat(stefan.getStringMap()).containsValue("val1");
        assertThat(boyd.getStringMap()).containsKey("key1");
        assertThat(boyd.getStringMap()).containsValue("val1");

        List<Person> persons = repository.findByStringMapContaining("key1", "al");

        assertThat(persons).contains(stefan, boyd);
    }

    @Test
    void findByMapKeyValueStartsWith() {
        assertThat(stefan.getStringMap()).containsKey("key1");
        assertThat(stefan.getStringMap()).containsValue("val1");
        assertThat(boyd.getStringMap()).containsKey("key1");
        assertThat(boyd.getStringMap()).containsValue("val1");

        List<Person> persons = repository.findByStringMapStartsWith("key1", "val");

        assertThat(persons).contains(stefan, boyd);
    }

    @Test
    void findByMapKeyValueGreaterThan() {
        assertThat(leroi.getIntMap()).containsKey("key2");
        assertThat(leroi.getIntMap().get("key2") > 0).isTrue();

        List<Person> persons = repository.findByIntMapGreaterThan("key2", 0);

        assertThat(persons).contains(leroi);
    }

    @Test
    void findByMapKeyValueLessThanOrEqual() {
        assertThat(leroi.getIntMap()).containsKey("key2");
        assertThat(leroi.getIntMap().get("key2") > 0).isTrue();

        List<Person> persons = repository.findByIntMapLessThanEqual("key2", 1);

        assertThat(persons).containsExactlyInAnyOrder(leroi, carter);
    }

    @Test
    void findByMapKeyValueBetween() {
        assertThat(leroi.getIntMap()).containsKey("key1");
        assertThat(leroi.getIntMap()).containsKey("key2");
        assertThat(leroi.getIntMap().get("key1") >= 0).isTrue();
        assertThat(leroi.getIntMap().get("key2") >= 0).isTrue();

        List<Person> persons = repository.findByIntMapBetween("key2", 0, 1);

        assertThat(persons).contains(leroi);
    }

    @Test
    void findByFirstNameContaining() {
        List<Person> persons = repository.findByFirstNameContaining("er");

        assertThat(persons).containsExactlyInAnyOrder(carter, oliver, leroi, leroi2);
    }

    @Test
    void findByAddressZipCodeContaining() {
        carter.setAddress(new Address("Foo Street 2", 2, "C10124", "C0123"));
        repository.save(carter);
        dave.setAddress(new Address("Foo Street 1", 1, "C10123", "Bar"));
        repository.save(dave);
        boyd.setAddress(new Address(null, null, null, null));
        repository.save(boyd);

        List<Person> persons = repository.findByAddressZipCodeContaining("C10");

        assertThat(persons).containsExactlyInAnyOrder(carter, dave);
    }

    @Test
    public void findPersonById() {
        Optional<Person> person = repository.findById(dave.getId());

        assertThat(person).hasValueSatisfying(actual -> {
            assertThat(actual).isInstanceOf(Person.class);
            assertThat(actual).isEqualTo(dave);
        });
    }

    @Test
    public void findAllMusicians() {
        List<Person> result = (List<Person>) repository.findAll();

        assertThat(result)
            .containsExactlyInAnyOrderElementsOf(all);
    }

    @Test
    public void findAllWithGivenIds() {
        List<Person> result = (List<Person>) repository.findAllById(Arrays.asList(dave.getId(), boyd.getId()));

        assertThat(result)
            .hasSize(2)
            .contains(dave)
            .doesNotContain(oliver, carter, stefan, leroi, alicia);
    }

    @Test
    public void findPersonsByLastname() {
        List<Person> result = repository.findByLastName("Beauford");

        assertThat(result)
            .hasSize(1)
            .containsOnly(carter);
    }

    @Test
    public void findPersonsSomeFieldsByLastnameProjection() {
        List<PersonSomeFields> result = repository.findPersonSomeFieldsByLastName("Beauford");

        assertThat(result)
            .hasSize(1)
            .containsOnly(carter.toPersonSomeFields());
    }

    @Test
    public void findDynamicTypeByLastnameDynamicProjection() {
        List<PersonSomeFields> result = repository.findByLastName("Beauford", PersonSomeFields.class);

        assertThat(result)
            .hasSize(1)
            .containsOnly(carter.toPersonSomeFields());
    }

    @Test
    public void findPersonsByFriendAge() {
        oliver.setFriend(alicia);
        repository.save(oliver);
        dave.setFriend(oliver);
        repository.save(dave);
        carter.setFriend(dave);
        repository.save(carter);
        assertThat(dave.getAge()).isEqualTo(42);

        List<Person> result = repository.findByFriendAge(42);

        assertThat(result)
            .hasSize(1)
            .containsExactly(carter);

        setFriendsToNull(oliver, dave, carter);
    }

    private void setFriendsToNull(Person... persons) {
        for (Person person : persons) {
            person.setFriend(null);
            repository.save(person);
        }
    }

    @Test
    public void findPersonsByFriendAgeNotEqual() {
        oliver.setFriend(alicia);
        repository.save(oliver);
        dave.setFriend(oliver);
        repository.save(dave);
        carter.setFriend(dave);
        repository.save(carter);

        List<Person> result = repository.findByFriendAgeIsNot(42);

        assertThat(result)
            .hasSize(2)
            .containsExactlyInAnyOrder(dave, oliver);

        setFriendsToNull(oliver, dave, carter);
    }

    @Test
    public void findPersonsByAddressZipCode() {
        String zipCode = "C012345";
        carter.setAddress(new Address("Foo Street 2", 2, "C012344", "C0123"));
        repository.save(carter);
        dave.setAddress(new Address("Foo Street 1", 1, zipCode, "Bar"));
        repository.save(dave);
        boyd.setAddress(new Address(null, null, null, null));
        repository.save(boyd);

        List<Person> result = repository.findByAddressZipCode(zipCode);

        assertThat(result)
            .hasSize(1)
            .containsExactly(dave);
    }

    @Test
    public void findPersonsByFriendAgeGreaterThan() {
        alicia.setFriend(boyd);
        repository.save(alicia);
        dave.setFriend(oliver);
        repository.save(dave);
        carter.setFriend(dave);
        repository.save(carter);
        leroi.setFriend(carter);
        repository.save(leroi);

        assertThat(alicia.getFriend().getAge()).isGreaterThan(42);
        assertThat(leroi.getFriend().getAge()).isGreaterThan(42);

        List<Person> result = repository.findByFriendAgeGreaterThan(42);

        assertThat(result)
            .hasSize(2)
            .containsExactlyInAnyOrder(alicia, leroi);

        setFriendsToNull(alicia, dave, carter, leroi);
    }

    @Test
    public void findPersonsByFriendAgeLessThanOrEqual() {
        alicia.setFriend(boyd);
        repository.save(alicia);
        dave.setFriend(oliver);
        repository.save(dave);
        carter.setFriend(dave);
        repository.save(carter);
        leroi.setFriend(carter);
        repository.save(leroi);

        List<Person> result = repository.findByFriendAgeLessThanEqual(42);

        assertThat(result)
            .hasSize(2)
            .containsExactlyInAnyOrder(dave, carter);

        setFriendsToNull(alicia, dave, carter, leroi);
    }

    @Test
    public void findAll_doesNotFindDeletedPersonByEntity() {
        try {
            repository.delete(dave);

            List<Person> result = (List<Person>) repository.findAll();

            assertThat(result)
                .doesNotContain(dave)
                .hasSize(all.size() - 1);
        } finally {
            repository.save(dave);
        }
    }

    @Test
    public void findAll_doesNotFindDeletedPersonById() {
        try {
            repository.deleteById(dave.getId());

            List<Person> result = (List<Person>) repository.findAll();

            assertThat(result)
                .doesNotContain(dave)
                .hasSize(all.size() - 1);
        } finally {
            repository.save(dave);
        }
    }

    @Test
    public void findPersonsByFirstname() {
        List<Person> result = repository.findByFirstName("Leroi");

        assertThat(result).hasSize(2).containsOnly(leroi, leroi2);
    }

    @Test
    public void findByLastnameNot_forExistingResult() {
        Stream<Person> result = repository.findByLastNameNot("Moore");

        assertThat(result)
            .doesNotContain(leroi, leroi2)
            .contains(dave, donny, oliver, carter, boyd, stefan, alicia);
    }

    @Test
    public void findByFirstnameNotIn_forEmptyResult() {
        Set<String> allFirstNames = all.stream().map(Person::getFirstName).collect(Collectors.toSet());
//		Stream<Person> result = repository.findByFirstnameNotIn(allFirstNames);
        assertThatThrownBy(() -> repository.findByFirstNameNotIn(allFirstNames))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unsupported keyword!");

//		assertThat(result).isEmpty();
    }

    @Test
    public void findByFirstnameNotIn_forExistingResult() {
//		Stream<Person> result = repository.findByFirstnameNotIn(Collections.singleton("Alicia"));
        assertThatThrownBy(() -> repository.findByFirstNameNotIn(Collections.singleton("Alicia")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unsupported keyword!");

//		assertThat(result).contains(dave, donny, oliver, carter, boyd, stefan, leroi, leroi2);
    }

    @Test
    public void findByFirstnameIn_forEmptyResult() {
        Stream<Person> result = repository.findByFirstNameIn(Arrays.asList("Anastasiia", "Daniil"));

        assertThat(result).isEmpty();
    }

    @Test
    public void findByFirstnameIn_forExistingResult() {
        Stream<Person> result = repository.findByFirstNameIn(Arrays.asList("Alicia", "Stefan"));

        assertThat(result).contains(alicia, stefan);
    }

    @Test
    public void countByLastName_forExistingResult() {
        assertThatThrownBy(() -> repository.countByLastName("Leroi"))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Query method Person.countByLastName not supported.");

//		assertThat(result).isEqualTo(2);
    }

    @Test
    public void countByLastName_forEmptyResult() {
        assertThatThrownBy(() -> repository.countByLastName("Smirnova"))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Query method Person.countByLastName not supported.");

//		assertThat(result).isEqualTo(0);
    }

    @Test
    public void findByAgeGreaterThan_forExistingResult() {
        Slice<Person> slice = repository.findByAgeGreaterThan(40, PageRequest.of(0, 10));

        assertThat(slice.hasContent()).isTrue();
        assertThat(slice.hasNext()).isFalse();
        assertThat(slice.getContent()).hasSize(4).contains(dave, carter, boyd, leroi);
    }

    @Test
    public void findPersonsSomeFieldsByAgeGreaterThan_forExistingResultProjection() {
        Slice<PersonSomeFields> slice = repository.findPersonSomeFieldsByAgeGreaterThan(
            40, PageRequest.of(0, 10)
        );

        assertThat(slice.hasContent()).isTrue();
        assertThat(slice.hasNext()).isFalse();
        assertThat(slice.getContent()).hasSize(4).contains(dave.toPersonSomeFields(),
            carter.toPersonSomeFields(), boyd.toPersonSomeFields(), leroi.toPersonSomeFields());
    }

    @Test
    public void findByAgeGreaterThan_respectsLimit() {
        Slice<Person> slice = repository.findByAgeGreaterThan(40, PageRequest.of(0, 1));

        assertThat(slice.hasContent()).isTrue();
        assertThat(slice.hasNext()).isFalse(); // TODO: not implemented yet. should be true instead
        assertThat(slice.getContent()).containsAnyOf(dave, carter, boyd, leroi).hasSize(1);
    }

    @Test
    public void findByAgeGreaterThan_respectsLimitAndOffsetAndSort() {
        List<Person> result = IntStream.range(0, 4)
            .mapToObj(index -> repository.findByAgeGreaterThan(40, PageRequest.of(
                index, 1, Sort.by("age")
            )))
            .flatMap(slice -> slice.getContent().stream())
            .collect(Collectors.toList());

        assertThat(result)
            .hasSize(4)
            .containsSequence(leroi, dave, boyd, carter);
    }

    @Test
    public void findByAgeGreaterThan_returnsValidValuesForNextAndPrev() {
        Slice<Person> first = repository.findByAgeGreaterThan(40, PageRequest.of(0, 1, Sort.by("age")));

        assertThat(first.hasContent()).isTrue();
        assertThat(first.getNumberOfElements()).isEqualTo(1);
        assertThat(first.hasNext()).isFalse(); // TODO: not implemented yet. should be true instead
        assertThat(first.isFirst()).isTrue();

        Slice<Person> last = repository.findByAgeGreaterThan(40, PageRequest.of(3, 1, Sort.by("age")));

        assertThat(last.hasContent()).isTrue();
        assertThat(last.getNumberOfElements()).isEqualTo(1);
        assertThat(last.hasNext()).isFalse();
        assertThat(last.isLast()).isTrue();
    }

    @Test
    public void findByAgeGreaterThan_forEmptyResult() {
        Slice<Person> slice = repository.findByAgeGreaterThan(100, PageRequest.of(0, 10));

        assertThat(slice.hasContent()).isFalse();
        assertThat(slice.hasNext()).isFalse();
        assertThat(slice.getContent()).isEmpty();
    }

    @Test
    public void findByLastNameStartsWithOrderByAgeAsc_respectsLimitAndOffset() {
        Page<Person> first = repository.findByLastNameStartsWithOrderByAgeAsc("Moo", PageRequest.of(0, 1));

        assertThat(first.getNumberOfElements()).isEqualTo(1);
        assertThat(first.getTotalPages()).isEqualTo(2);
        assertThat(first.get()).hasSize(1).containsOnly(leroi2);

        Page<Person> last = repository.findByLastNameStartsWithOrderByAgeAsc("Moo", first.nextPageable());

        assertThat(last.getTotalPages()).isEqualTo(2);
        assertThat(last.getNumberOfElements()).isEqualTo(1);
        assertThat(last.get()).hasSize(1).containsAnyOf(leroi);

        Page<Person> all = repository.findByLastNameStartsWithOrderByAgeAsc("Moo", PageRequest.of(0, 5));

        assertThat(all.getTotalPages()).isEqualTo(1);
        assertThat(all.getNumberOfElements()).isEqualTo(2);
        assertThat(all.get()).hasSize(2).containsOnly(leroi, leroi2);
    }

    @Test
    public void findPersonsByFirstnameAndByAge() {
        List<Person> result = repository.findByFirstNameAndAge("Leroi", 25);
        assertThat(result).containsOnly(leroi2);

        result = repository.findByFirstNameAndAge("Leroi", 41);
        assertThat(result).containsOnly(leroi);
    }

    @Test
    public void findPersonsByFirstnameStartsWith() {
        List<Person> result = repository.findByFirstNameStartsWith("D");

        assertThat(result).containsOnly(dave, donny, douglas);
    }

    @Test
    public void findPersonsByFriendFirstnameStartsWith() {
        dave.setFriend(oliver);
        repository.save(dave);
        carter.setFriend(dave);
        repository.save(carter);

        List<Person> result = repository.findByFriendFirstNameStartsWith("D");

        assertThat(result)
            .hasSize(1)
            .containsExactly(carter);

        setFriendsToNull(dave, carter);
    }

    @Test
    public void findPagedPersons() {
        Page<Person> result = repository.findAll(PageRequest.of(
            1, 2, Sort.Direction.ASC, "lastname", "firstname")
        );
        assertThat(result.isFirst()).isFalse();
        assertThat(result.isLast()).isFalse();
    }

    @Test
    public void findPersonInAgeRangeCorrectly() {
        Iterable<Person> it = repository.findByAgeBetween(40, 45);

        assertThat(it).hasSize(3).contains(dave);
    }

    @Test
    public void findPersonInAgeRangeCorrectlyOrderByLastName() {
        Iterable<Person> it = repository.findByAgeBetweenOrderByLastName(30, 45);

        assertThat(it).hasSize(6);
    }

    @Test
    public void findPersonInAgeRangeAndNameCorrectly() {
        Iterable<Person> it = repository.findByAgeBetweenAndLastName(40, 45, "Matthews");
        assertThat(it).hasSize(1);

        Iterable<Person> result = repository.findByAgeBetweenAndLastName(20, 26, "Moore");
        assertThat(result).hasSize(1);
    }

    @Test
    public void findPersonsByFriendsInAgeRangeCorrectly() {
        oliver.setFriend(alicia);
        repository.save(oliver);
        dave.setFriend(oliver);
        repository.save(dave);
        carter.setFriend(dave);
        repository.save(carter);

        List<Person> result = repository.findByFriendAgeBetween(40, 45);

        assertThat(result)
            .hasSize(1)
            .containsExactly(carter);

        setFriendsToNull(oliver, dave, carter);
    }

    @Test
    public void findPersonsByAddress() {
        if (IndexUtils.isFindByPojoSupported(client)) {
            Address address = new Address("Foo Street 1", 1, "C0123", "Bar");
            dave.setAddress(address);
            repository.save(dave);

            List<Person> persons = repository.findByAddress(address);
            assertThat(persons).containsOnly(dave);
        }
    }

    @Test
    public void findPersonsByFriend() {
        if (IndexUtils.isFindByPojoSupported(client)) {
            alicia.setAddress(new Address("Foo Street 1", 1, "C0123", "Bar"));
            repository.save(alicia);
            oliver.setFriend(alicia);
            repository.save(oliver);

            List<Person> persons = repository.findByFriend(alicia);
            assertThat(persons).containsOnly(oliver);
        }
    }

    @Test
    public void findPersonsByFriendAddress() {
        if (IndexUtils.isFindByPojoSupported(client)) {
            Address address = new Address("Foo Street 1", 1, "C0123", "Bar");
            dave.setAddress(address);
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendAddress(address);

            assertThat(result)
                .hasSize(1)
                .containsExactly(carter);

            setFriendsToNull(carter);
        }
    }

    @Test
    public void findPersonsByFriendAddressZipCode() {
        String zipCode = "C012345";
        Address address = new Address("Foo Street 1", 1, zipCode, "Bar");
        dave.setAddress(address);
        repository.save(dave);

        carter.setFriend(dave);
        repository.save(carter);

        List<Person> result = repository.findByFriendAddressZipCode(zipCode);

        assertThat(result)
            .hasSize(1)
            .containsExactly(carter);

        setFriendsToNull(carter);
    }

    @Test
    public void findPersonsByFriendFriendAddressZipCode() {
        String zipCode = "C0123";
        Address address = new Address("Foo Street 1", 1, zipCode, "Bar");
        dave.setAddress(address);
        repository.save(dave);

        carter.setFriend(dave);
        repository.save(carter);
        oliver.setFriend(carter);
        repository.save(oliver);

        List<Person> result = repository.findByFriendFriendAddressZipCode(zipCode);

        assertThat(result)
            .hasSize(1)
            .containsExactly(oliver);

        setFriendsToNull(carter, oliver);
    }

    @Test
    // find by deeply nested String POJO field
    public void findPersonsByFriendFriendFriendFriendFriendFriendFriendFriendBestFriendFriendAddressZipCode() {
        String zipCode = "C0123";
        Address address = new Address("Foo Street 1", 1, zipCode, "Bar");
        dave.setAddress(address);
        repository.save(dave);

        alicia.setFriend(dave);
        repository.save(alicia);
        oliver.setBestFriend(alicia);
        repository.save(oliver);
        carter.setFriend(oliver);
        repository.save(carter);
        donny.setFriend(carter);
        repository.save(donny);
        boyd.setFriend(donny);
        repository.save(boyd);
        stefan.setFriend(boyd);
        repository.save(stefan);
        leroi.setFriend(stefan);
        repository.save(leroi);
        leroi2.setFriend(leroi);
        repository.save(leroi2);
        matias.setFriend(leroi2);
        repository.save(matias);
        douglas.setFriend(matias);
        repository.save(douglas);

        List<Person> result =
            repository.findByFriendFriendFriendFriendFriendFriendFriendFriendBestFriendFriendAddressZipCode(zipCode);

        assertThat(result)
            .hasSize(1)
            .containsExactly(douglas);

        setFriendsToNull(all.toArray(Person[]::new));
    }

    @Test
    // find by deeply nested Integer POJO field
    public void findPersonsByFriendFriendFriendFriendFriendFriendFriendFriendBestFriendAddressApartmentNumber() {
        int apartment = 10;
        Address address = new Address("Foo Street 1", apartment, "C0123", "Bar");
        alicia.setAddress(address);
        repository.save(alicia);

        oliver.setBestFriend(alicia);
        repository.save(oliver);
        carter.setFriend(oliver);
        repository.save(carter);
        donny.setFriend(carter);
        repository.save(donny);
        boyd.setFriend(donny);
        repository.save(boyd);
        stefan.setFriend(boyd);
        repository.save(stefan);
        leroi.setFriend(stefan);
        repository.save(leroi);
        leroi2.setFriend(leroi);
        repository.save(leroi2);
        douglas.setFriend(leroi2);
        repository.save(douglas);
        matias.setFriend(douglas);
        repository.save(matias);

        List<Person> result =
            repository.findByFriendFriendFriendFriendFriendFriendFriendFriendBestFriendAddressApartment(apartment);

        assertThat(result)
            .hasSize(1)
            .containsExactly(matias);

        setFriendsToNull(all.toArray(Person[]::new));
    }

    @Test
    // find by deeply nested POJO
    public void findPersonsByFriendFriendFriendFriendFriendFriendFriendFriendBestFriendBestFriendAddress() {
        if (IndexUtils.isFindByPojoSupported(client)) {
            Address address = new Address("Foo Street 1", 1, "C0123", "Bar");
            dave.setAddress(address);
            repository.save(dave);

            alicia.setBestFriend(dave);
            repository.save(alicia);
            oliver.setBestFriend(alicia);
            repository.save(oliver);
            carter.setFriend(oliver);
            repository.save(carter);
            donny.setFriend(carter);
            repository.save(donny);
            boyd.setFriend(donny);
            repository.save(boyd);
            stefan.setFriend(boyd);
            repository.save(stefan);
            leroi.setFriend(stefan);
            repository.save(leroi);
            matias.setFriend(leroi);
            repository.save(matias);
            douglas.setFriend(matias);
            repository.save(douglas);
            leroi2.setFriend(douglas);
            repository.save(leroi2);

            List<Person> result =
                repository.findByFriendFriendFriendFriendFriendFriendFriendFriendBestFriendBestFriendAddress(address);

            assertThat(result)
                .hasSize(1)
                .containsExactly(leroi2);

            setFriendsToNull(all.toArray(Person[]::new));
        }
    }
}
