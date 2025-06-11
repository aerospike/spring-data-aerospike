package org.springframework.data.aerospike.repository.query.blocking.find;

import com.aerospike.client.Value;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.repository.query.blocking.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.PersonId;
import org.springframework.data.aerospike.sample.PersonSomeFields;
import org.springframework.data.aerospike.util.TestUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.testcontainers.shaded.com.google.common.collect.Streams;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.query.QueryParam.of;

/**
 * Tests for the "Equals" repository query. Keywords: Is, Equals (or no keyword).
 */
public class EqualsTests extends PersonRepositoryQueryTests {

    @Test
    void findBySimplePropertyEquals_String() {
        List<Person> result = repository.findByFirstName("Leroi");
        assertThat(result).containsOnly(leroi, leroi2);

        List<Person> result1 = repository.findByFirstNameIgnoreCase("lEroi");
        assertThat(result1).containsOnly(leroi, leroi2);

        List<Person> result2 = repository.findByFirstNameIs("lEroi"); // another way to call the query method
        assertThat(result2).hasSize(0);

        List<Person> result3 = repository.findByFirstNameEquals("leroi "); // another way to call the query method
        assertThat(result3).hasSize(0);
    }

    @Test
    void findBySimpleProperty_String_projection() {
        List<PersonSomeFields> result = repository.findPersonSomeFieldsByLastName("Beauford");
        assertThat(result).containsOnly(carter.toPersonSomeFields());
    }

    @Test
    void findBySimpleProperty_id_projection() {
        List<PersonId> result = repository.findPersonIdByFirstName("Carter");
        assertThat(result).containsOnly(carter.toPersonId());
    }

    @Test
    void findBySimplePropertyEquals_String_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByFirstName(100))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.firstName EQ: Type mismatch, expecting String");

        assertThatThrownBy(() -> negativeTestsRepository.findByLastName("Beauford", "Boford"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.lastName EQ: invalid number of arguments, expecting one");
    }

    @Test
    void findBySimplePropertyEquals_Enum() {
        List<Person> result = repository.findByGender(Person.Gender.FEMALE);
        assertThat(result).containsOnly(alicia);
    }

    @Test
    void findBySimplePropertyEquals_BooleanInt() {
        boolean initialValue = Value.UseBoolBin;
        Value.UseBoolBin = false; // save boolean as int
        Person intBoolBinPerson = Person.builder()
            .id(BaseIntegrationTests.nextId())
            .isActive(true)
            .firstName("Test")
            .build();
        repository.save(intBoolBinPerson);

        List<Person> persons = repository.findByIsActive(true);
        assertThat(persons).contains(intBoolBinPerson);

        Value.UseBoolBin = initialValue; // set back to the default value
        repository.delete(intBoolBinPerson);
    }

    @Test
    void findBySimplePropertyEquals_BooleanInt_NegativeTest() {
        boolean initialValue = Value.UseBoolBin;
        Value.UseBoolBin = false; // save boolean as int

        assertThatThrownBy(() -> negativeTestsRepository.findByIsActive())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.isActive EQ: invalid number of arguments, expecting one");

        assertThatThrownBy(() -> negativeTestsRepository.findByIsActive(true, false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.isActive EQ: invalid number of arguments, expecting one");

        Value.UseBoolBin = initialValue; // set back to the default value
    }

    @Test
    void findBySimplePropertyEquals_Boolean() {
        boolean initialValue = Value.UseBoolBin;
        Value.UseBoolBin = true; // save boolean as bool, available in Server 5.6+
        Person intBoolBinPerson = Person.builder().id(BaseIntegrationTests.nextId()).isActive(true).firstName("Test")
            .build();
        repository.save(intBoolBinPerson);

        List<Person> persons = repository.findByIsActive(true);
        assertThat(persons).contains(intBoolBinPerson);

        Value.UseBoolBin = initialValue; // set back to the default value
        repository.delete(intBoolBinPerson);
    }

    @Test
    void findBySimplePropertyEquals_Boolean_NegativeTest() {
        boolean initialValue = Value.UseBoolBin;
        Value.UseBoolBin = true; // save boolean as bool, available in Server 5.6+

        assertThatThrownBy(() -> negativeTestsRepository.findByIsActive())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.isActive EQ: invalid number of arguments, expecting one");

        assertThatThrownBy(() -> negativeTestsRepository.findByIsActive(true, false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.isActive EQ: invalid number of arguments, expecting one");

        Value.UseBoolBin = initialValue; // set back to the default value
    }

    @Test
    void findById() {
        Optional<Person> result = repository.findById(dave.getId());
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(dave);
    }

    @Test
    void findById_AND_simpleProperty() {
        QueryParam ids = of(dave.getId());
        QueryParam name = of(carter.getFirstName());
        List<Person> persons = repository.findByIdAndFirstName(ids, name);
        assertThat(persons).isEmpty();

        ids = of(dave.getId());
        name = of(dave.getFirstName());
        persons = repository.findByIdAndFirstName(ids, name);
        assertThat(persons).containsOnly(dave);

        ids = of(List.of(boyd.getId(), dave.getId(), carter.getId()));
        name = of(dave.getFirstName());
        // when in a combined query, "findById" part can receive multiple ids wrapped in QueryParam
        persons = repository.findByIdAndFirstName(ids, name);
        assertThat(persons).containsOnly(dave);

        ids = of(List.of(leroi.getId(), leroi2.getId(), carter.getId()));
        QueryParam firstName = of(leroi.getFirstName());
        QueryParam age = of(leroi2.getAge());
        List<Person> persons2 = repository.findByIdAndFirstNameAndAge(ids, firstName, age);
        assertThat(persons2).containsOnly(leroi2);

        // "findByIdOr..." will return empty list because primary keys are used to firstly narrow down the results.parts
        // In a combined id query "OR" can be put only between the non-id fields like shown below,
        // and the results are limited by the given ids
        ids = of(List.of(leroi.getId(), leroi2.getId(), carter.getId()));
        firstName = of(leroi.getFirstName());
        age = of(leroi2.getAge());
        List<Person> persons3 = repository.findByIdAndFirstNameOrAge(ids, firstName, age);
        assertThat(persons3).containsOnly(leroi, leroi2);

        ids = of(List.of(leroi.getId(), leroi2.getId(), carter.getId()));
        firstName = of(leroi.getFirstName());
        age = of(stefan.getAge());
        List<Person> persons4 = repository.findByIdAndFirstNameOrAge(ids, firstName, age);
        assertThat(persons4).containsOnly(leroi, leroi2);
    }

    @Test
    void findById_dynamicProjection() {
        List<PersonSomeFields> result = repository.findById(dave.getId(), PersonSomeFields.class);
        assertThat(result).containsOnly(dave.toPersonSomeFields());
    }

    @Test
    void findById_AND_simpleProperty_dynamicProjection() {
        QueryParam ids = of(List.of(boyd.getId(), dave.getId(), carter.getId()));
        QueryParam lastName = of(carter.getLastName());
        List<PersonSomeFields> result = repository.findByIdAndLastName(ids, lastName, PersonSomeFields.class);
        assertThat(result).containsOnly(carter.toPersonSomeFields());
    }

    @Test
    void findById_AND_simpleProperty_DynamicProjection_EmptyResult() {
        QueryParam ids = of(List.of(carter.getId(), boyd.getId()));
        QueryParam lastName = of(dave.getLastName());
        List<PersonSomeFields> result = repository.findByIdAndLastName(ids, lastName, PersonSomeFields.class);
        assertThat(result).isEmpty();
    }

    @Test
    void findBySimpleProperty_AND_id_dynamicProjection() {
        QueryParam id = of(dave.getId());
        QueryParam lastName = of(dave.getLastName());
        List<PersonSomeFields> result = repository.findByLastNameAndId(lastName, id, PersonSomeFields.class);
        assertThat(result).containsOnly(dave.toPersonSomeFields());
    }

    @Test
    void findBySimpleProperty_AND_simpleProperty_DynamicProjection() {
        QueryParam firstName = of(carter.getFirstName());
        QueryParam lastName = of(carter.getLastName());
        List<PersonSomeFields> result = repository.findByFirstNameAndLastName(firstName, lastName,
            PersonSomeFields.class);
        assertThat(result).containsOnly(carter.toPersonSomeFields());
    }

    @Test
    void findAllByIdsIterable_shouldReturnAllExisting() {
        Iterable<Person> result = repository.findAllById(List.of(dave.getId(), carter.getId()));
        assertThat(result).containsExactlyInAnyOrder(dave, carter);

        Iterable<Person> result2 = repository.findAllById(List.of("1", "2"));
        assertThat(result2).isEmpty();

        Iterable<Person> result3 = repository.findAllById(List.of("1", "2", dave.getId(), carter.getId()));
        assertThat(result3).containsExactlyInAnyOrder(dave, carter);
    }

    @Test
    void findAllByIds_paginatedQuery() {
        List<String> ids = allPersons.stream().map(Person::getId).toList();
        Page<Person> result = repository.findAllById(ids, Pageable.ofSize(11));
        assertThat(result.getTotalPages()).isEqualTo(1);
        assertThat(result).hasSameElementsAs(allPersons);

        Page<Person> result2 = repository.findAllById(ids, PageRequest.ofSize(10));
        assertThat(result2.getTotalPages()).isEqualTo(2);

        Page<Person> result3 = repository.findAllById(ids, PageRequest.of(1, 2, Sort.by("firstName")));
        assertThat(result3.getTotalPages()).isEqualTo(6);
        Iterator<Person> iterator = result3.iterator();
        assertThat(iterator.next()).isEqualTo(carter);
        assertThat(iterator.next()).isEqualTo(dave);
        assertThat(iterator.hasNext()).isFalse();

        Page<Person> result4 = repository.findAllById(ids, Pageable.unpaged());
        assertThat(result4.getTotalPages()).isEqualTo(1);
        assertThat(result4.getTotalElements()).isEqualTo(11);
    }

    @Test
    void findAllByIds_paginatedQuery_withSorting_shouldReturnAllExisting() {
        var idsIncludingNonExistent = Streams.concat(allPersons.stream().map(Person::getId), Stream.of("1", "2")).toList();
        Page<Person> result = repository.findAllById(
            idsIncludingNonExistent,
            PageRequest.of(1, 2, Sort.by("firstName"))
        );
        // The necessary slice is chosen after retrieving all results
        // That's why the total page count is 6 here and not 7 (two non-existent ids, no results retrieved for them)
        assertThat(result.getTotalPages()).isEqualTo(6);
        Iterator<Person> iterator = result.iterator();
        assertThat(iterator.next()).isEqualTo(carter);
        assertThat(iterator.next()).isEqualTo(dave);
        assertThat(iterator.hasNext()).isFalse();

        var idsOnlyNonExistent = List.of("1", "2");
        Page<Person> result3 = repository.findAllById(
            idsOnlyNonExistent,
            PageRequest.of(1, 2, Sort.by("firstName"))
        );
        // The necessary slice is chosen after retrieving all results
        // That's why the total page count is 0 (no existing ids, no results retrieved)
        assertThat(result3.getTotalPages()).isEqualTo(0);
        Iterator<Person> iterator3 = result3.iterator();
        assertThat(iterator3.hasNext()).isFalse();
    }

    @Test
    void findAllByIds_paginatedQuery_withOffset_originalOrder_unsorted() {
        List<String> ids = allPersons.stream().map(Person::getId).toList();
        assertThat(ids.size()).isEqualTo(11);
        assertThat(ids.indexOf(oliver.getId())).isEqualTo(2);
        assertThat(ids.indexOf(alicia.getId())).isEqualTo(3);

        // Paginated queries with offset and no sorting (i.e. original order in ids collection)
        // are only allowed for purely id queries
        Page<Person> result1 = repository.findAllById(ids, PageRequest.of(1, 2));
        assertThat(result1.getTotalPages()).isEqualTo(6); // Overall ids quantity is 11
        Iterator<Person> iterator = result1.iterator();
        assertThat(iterator.next()).isEqualTo(oliver);
        assertThat(iterator.next()).isEqualTo(alicia);
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    void findAllByIds_paginatedQuery_unsorted_shouldReturnAllExisting() {
        List<String> ids = allPersons.stream().map(Person::getId).toList();
        var idsIncludingNonExistent = Streams.concat(ids.stream(), Stream.of("1", "2")).toList();
        Page<Person> result = repository.findAllById(
            idsIncludingNonExistent,
            PageRequest.of(1, 2)
        );
        // Overall ids quantity is 13, with 11 existing in DB, so total existing pages count is 6
        assertThat(result.getTotalPages()).isEqualTo(6);
        Iterator<Person> iterator = result.iterator();
        assertThat(iterator.next()).isEqualTo(oliver);
        assertThat(iterator.next()).isEqualTo(alicia);
        assertThat(iterator.hasNext()).isFalse();

        Page<Person> result2 = repository.findAllById(
            idsIncludingNonExistent,
            PageRequest.of(6, 2)
        );
        // Overall ids quantity is 13, with 11 existing in DB, so total existing pages count is 6
        assertThat(result2.getTotalPages()).isEqualTo(6);
        // This page has no content because of two ids that don't exist in DB
        assertThat(result2.hasContent()).isFalse();
        Iterator<Person> iterator2 = result2.iterator();
        assertThat(iterator2.hasNext()).isFalse();

        var idsOnlyNonExistent = List.of("1", "2");
        Page<Person> result3 = repository.findAllById(
            idsOnlyNonExistent,
            PageRequest.of(1, 2)
        );
        // No existing pages (because two ids provided don't exist in DB)
        assertThat(result3.getTotalPages()).isEqualTo(0);
        assertThat(result3.getContent()).isEmpty();
        Iterator<Person> iterator3 = result3.iterator();
        assertThat(iterator3.hasNext()).isFalse();
    }

    @Test
    void findAllByIds_sorted() {
        List<String> ids = allPersons.stream().map(Person::getId).toList();
        List<Person> result = repository.findAllById(ids, Sort.by(Sort.Direction.DESC, "firstName"));
        assertThat(result).hasSameElementsAs(allPersons);
        assertThat(result.iterator().next()).isEqualTo(stefan);

        List<Person> result2 = repository.findAllById(ids, Sort.by(Sort.Direction.ASC, "firstName"));
        assertThat(result2).hasSameElementsAs(allPersons);
        assertThat(result2.iterator().next()).isEqualTo(alicia);
    }

    @Test
    void findAllByIds_AND_simpleProperty() {
        QueryParam ids1 = of(List.of(dave.getId(), boyd.getId()));
        QueryParam name1 = of(dave.getFirstName());
        List<Person> persons1 = repository.findAllByIdAndFirstName(ids1, name1);
        assertThat(persons1).contains(dave);

        QueryParam ids2 = of(List.of(dave.getId(), boyd.getId()));
        QueryParam name2 = of(carter.getFirstName());
        List<Person> persons2 = repository.findAllByIdAndFirstName(ids2, name2);
        assertThat(persons2).isEmpty();
    }

    @Test
    void findAllByIds_AND_simpleProperty_paginated() {
        QueryParam ids = of(List.of(dave.getId(), boyd.getId()));
        QueryParam names = of(List.of(dave.getFirstName(), boyd.getFirstName()));
        Slice<Person> persons1 = repository.findAllByIdAndFirstNameIn(ids, names, Pageable.ofSize(1));
        assertThat(persons1.getSize()).isEqualTo(1);
        assertThat(persons1.getContent()).containsOnly(dave);
        assertThat(persons1.hasNext()).isTrue();

        Slice<Person> persons2 = repository.findAllByIdAndFirstNameIn(ids, names, Pageable.unpaged());
        assertThat(persons2.getSize()).isEqualTo(2);
        assertThat(persons2.getContent()).containsExactlyInAnyOrder(boyd, dave);
        assertThat(persons2.hasNext()).isFalse();

        Slice<Person> persons3 = repository.findAllByIdAndFirstNameIn(ids, names, PageRequest.of(1, 1, 
            Sort.by("firstName")));
        assertThat(persons3.getSize()).isEqualTo(1);
        assertThat(persons3.getContent()).containsOnly(dave); // it is the second result out of the given two
        assertThat(persons3.hasNext()).isFalse();

        QueryParam idsAll = of(allPersons.stream().map(Person::getId).toList());
        QueryParam namesAll = of(allPersons.stream().map(Person::getFirstName).toList());
        Slice<Person> persons4 = repository.findAllByIdAndFirstNameIn(idsAll, namesAll,
            PageRequest.of(1, 1, Sort.by("firstName")));
        assertThat(persons4.getSize()).isEqualTo(1);
        assertThat(persons4.getContent()).containsOnly(boyd);
        assertThat(persons4.hasNext()).isTrue();
    }

    @Test
    void findAllByIds_AND_simpleProperty_paginated_shouldReturnAllExisting() {
        QueryParam ids = of(List.of("1", "2", dave.getId(), boyd.getId()));
        QueryParam names = of(List.of(dave.getFirstName(), boyd.getFirstName(), "testName"));

        Slice<Person> persons = repository.findAllByIdAndFirstNameIn(ids, names,
            PageRequest.of(1, 1, Sort.by("firstName")));
        assertThat(persons.getSize()).isEqualTo(1);
        assertThat(persons.getContent()).containsOnly(dave); // it is the second result out of the given two
        assertThat(persons.hasNext()).isFalse();

        QueryParam idsNonExistent = of(List.of("1", "2"));
        Slice<Person> persons3 = repository.findAllByIdAndFirstNameIn(idsNonExistent, names,
            PageRequest.of(1, 1, Sort.by("firstName")));
        // No existing records, size being 1 is a result of Slice's getSize() returning page size in this case
        assertThat(persons3.getSize()).isEqualTo(1);
        assertThat(persons3.getContent()).isEmpty(); // No existing ids
        assertThat(persons3.hasNext()).isFalse();
    }

    @Test
    void findAllByIds_AND_simpleProperty_sorted() {
        QueryParam ids = of(List.of(douglas.getId(), dave.getId(), boyd.getId()));
        QueryParam names = of(List.of(douglas.getFirstName(), dave.getFirstName(), boyd.getFirstName()));
        List<Person> persons = repository.findAllByIdAndFirstNameIn(ids, names, 
            Sort.by(Sort.Direction.DESC, "firstName"));
        assertThat(persons.get(0)).isEqualTo(douglas);

        List<Person> persons2 = repository.findAllByIdAndFirstNameIn(ids, names, 
            Sort.by(Sort.Direction.ASC, "firstName"));
        assertThat(persons2.get(0)).isEqualTo(boyd);
    }

    @Test
    void findByNestedSimplePropertyEquals() {
        String zipCode = "C0124";
        assertThat(carter.getAddress().getZipCode()).isEqualTo(zipCode);
        assertThat(repository.findByAddressZipCode(zipCode)).containsExactly(carter);

        zipCode = "C012345";
        Address address = new Address("Foo Street 1", 1, zipCode, "Bar");
        dave.setAddress(address);
        repository.save(dave);

        carter.setFriend(dave);
        repository.save(carter);

        List<Person> result = repository.findByFriendAddressZipCode(zipCode);
        assertThat(result).containsExactly(carter);

        // An alternative to "findByFriendAddressZipCode" is using a custom query
        Qualifier nestedZipCodeEq = Qualifier.builder()
            // find records having a map with a key that equals a value
            // POJOs are saved as Maps
            .setFilterOperation(FilterOperation.MAP_VAL_EQ_BY_KEY) // POJOs are saved as Maps
            .setPath("friend.address.zipCode") // path includes bin name, context and the required map key
            .setValue(zipCode) // value of the nested key
            .build();

        Iterable<Person> result2 = repository.findUsingQuery(new Query(nestedZipCodeEq));
        assertThat(result).isEqualTo(result2);
        TestUtils.setFriendsToNull(repository, carter);
    }

    @Test
    void findByNestedSimplePropertyEquals_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByFriendAddressZipCode())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Address.zipCode EQ: invalid number of arguments, expecting one");

        assertThatThrownBy(() -> negativeTestsRepository.findByFriendAddressZipCodeEquals())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Address.zipCode EQ: invalid number of arguments, expecting one");
    }

    private void setSequenceOfFriends() {
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
    }

    // find by deeply nested String POJO field
    @Test
    void findByDeeplyNestedSimplePropertyEquals_PojoField_String_10_levels() {
        String zipCode = "C0123";
        Address address = new Address("Foo Street 1", 1, zipCode, "Bar");
        alicia.setAddress(address);
        repository.save(alicia);

        setSequenceOfFriends();

        List<Person> result =
            repository.findByFriendFriendFriendFriendFriendFriendFriendFriendBestFriendAddressZipCode(zipCode);
        assertThat(result).containsExactly(douglas);

        // An alternative way to perform the same using a custom query
        Qualifier nestedZipCodeEq = Qualifier.builder()
            // find records having a map with a key that equals a value
            // POJOs are saved as Maps
            .setFilterOperation(FilterOperation.MAP_VAL_EQ_BY_KEY) // POJOs are saved as Maps
            // path includes bin name, context and the required map key
            .setPath("friend.friend.friend.friend.friend.friend.friend.friend.bestFriend.address.zipCode")
            .setValue(zipCode) // value of the nested key
            .build();

        Iterable<Person> result2 = repository.findUsingQuery(new Query(nestedZipCodeEq));
        assertThat(result).isEqualTo(result2);

        // cleanup
        TestUtils.setFriendsToNull(repository, allPersons.toArray(Person[]::new));
        alicia.setAddress(null);
        repository.save(alicia);
    }

    // find by deeply nested Integer POJO field
    @Test
    void findByDeeplyNestedSimplePropertyEquals_PojoField_Integer_10_levels() {
        int apartment = 10;
        Address address = new Address("Foo Street 1", apartment, "C0123", "Bar");
        alicia.setAddress(address);
        repository.save(alicia);

        setSequenceOfFriends();

        List<Person> result =
            repository.findByFriendFriendFriendFriendFriendFriendFriendFriendBestFriendAddressApartment(apartment);
        assertThat(result).containsExactly(douglas);

        // An alternative way to perform the same using a custom query
        Qualifier nestedApartmentEq = Qualifier.builder()
            // find records having a map with a key that equals a value
            // POJOs are saved as Maps
            .setFilterOperation(FilterOperation.MAP_VAL_EQ_BY_KEY) // POJOs are saved as Maps
            .setPath("friend.friend.friend.friend.friend.friend.friend.friend.bestFriend.address.apartment") // path
            .setValue(apartment) // value of the nested key
            .build();

        Iterable<Person> result2 = repository.findUsingQuery(new Query(nestedApartmentEq));
        assertThat(result).isEqualTo(result2);

        // cleanup
        TestUtils.setFriendsToNull(repository, allPersons.toArray(Person[]::new));
        alicia.setAddress(null);
        repository.save(alicia);
    }

    // find by deeply nested POJO
    @Test
    void findByDeeplyNestedSimplePropertyEquals_Pojo_9_levels() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            Address address = new Address("Foo Street 1", 1, "C0123", "Bar");
            alicia.setAddress(address);
            repository.save(alicia);

            setSequenceOfFriends();

            List<Person> result =
                repository.findByFriendFriendFriendFriendFriendFriendFriendFriendBestFriendAddress(address);
            assertThat(result).containsExactly(douglas);

            // An alternative way to perform the same using a custom query
            Qualifier nestedAddressEq = Qualifier.builder()
                // find records having a map with a key that equals a value
                // POJOs are saved as Maps
                .setFilterOperation(FilterOperation.MAP_VAL_EQ_BY_KEY) // POJOs are saved as Maps
                .setPath("friend.friend.friend.friend.friend.friend.friend.friend.bestFriend.address") // path
                .setValue(pojoToMap(address)) // value of the nested key
                .build();

            Iterable<Person> result2 = repository.findUsingQuery(new Query(nestedAddressEq));
            assertThat(result).isEqualTo(result2);

            // cleanup
            TestUtils.setFriendsToNull(repository, allPersons.toArray(Person[]::new));
            alicia.setAddress(null);
            repository.save(alicia);
        }
    }

    @Test
    void findByCollectionEquals() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            List<String> listToCompareWith = List.of("str0", "str1", "str2");
            Assertions.assertThat(dave.getStrings()).isEqualTo(listToCompareWith);

            List<Person> persons = repository.findByStringsEquals(listToCompareWith);
            assertThat(persons).contains(dave);

            // another way to call the method
            List<Person> persons2 = repository.findByStrings(listToCompareWith);
            assertThat(persons2).contains(dave);

            List<Person> persons3 = repository.findByIntArray(new int[]{0, 1, 2, 3, 4, 5});
            assertThat(persons3).containsOnly(matias);

            List<Person> persons4 = repository.findByByteArray(new byte[]{1, 0, 1, 1, 0, 0, 0, 1});
            assertThat(persons4).containsOnly(stefan);
        }
    }

    @Test
    void findByCollectionEquals_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByStrings())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.strings EQ: invalid number of arguments, expecting one");

        assertThatThrownBy(() -> negativeTestsRepository.findByStringsEquals("string1", "string2"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.strings EQ: invalid number of arguments, expecting one");

        assertThatThrownBy(() -> negativeTestsRepository.findByStrings(List.of("test"), List.of("test2")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.strings EQ: invalid number of arguments, expecting one");
    }

    @Test
    void findByNestedCollectionEquals() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            var ints = List.of(1, 2, 3, 4);
            dave.setInts(ints);
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendInts(ints);

            assertThat(result).contains(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    void findByMapEquals() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            Map<String, String> mapToCompareWith = Map.of("key1", "val1", "key2", "val2");
            assertThat(boyd.getStringMap()).isEqualTo(mapToCompareWith);

            List<Person> persons = repository.findByStringMapEquals(mapToCompareWith);
            assertThat(persons).contains(boyd);

            // another way to call the method
            List<Person> persons2 = repository.findByStringMap(mapToCompareWith);
            assertThat(persons2).contains(boyd);
        }
    }

    @Test
    void findByNestedMapEquals() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            var intMap = Map.of("1", 2, "3", 4);
            dave.setIntMap(intMap);
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendIntMap(intMap);

            assertThat(result).contains(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    void findByMapEquals_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByStringMapEquals("map1"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.stringMap EQ: invalid argument type, expecting Map");

        assertThatThrownBy(() -> negativeTestsRepository.findByStringMap(100))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.stringMap EQ: invalid argument type, expecting Map");

        assertThatThrownBy(() -> negativeTestsRepository.findByStringMapEquals(Map.of("key", "value"), Map.of("key",
            "value")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.stringMap EQ: invalid number of arguments, expecting one");
    }

    @Test
    void findByPOJOEquals() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            alicia.setAddress(new Address("Foo Street 1", 1, "C0123", "Bar"));
            repository.save(alicia);
            oliver.setFriend(alicia);
            repository.save(oliver);

            List<Person> persons = repository.findByFriend(alicia);
            assertThat(persons).containsOnly(oliver);

            alicia.setAddress(null);
            repository.save(alicia);
        }
    }

    @Test
    void findByNestedPOJOEquals() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            Address address = new Address("Foo Street 1", 1, "C0123", "Bar");
            dave.setAddress(address);
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendAddress(address);

            assertThat(result).contains(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    void findByNestedPojoEquals_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByFriendAddress())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.address EQ: invalid number of arguments, expecting one POJO");

        assertThatThrownBy(() -> negativeTestsRepository.findByFriendAddressEquals())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.address EQ: invalid number of arguments, expecting one POJO");

        assertThatThrownBy(() -> negativeTestsRepository.findByFriendAddress(100))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.address EQ: Type mismatch, expecting Address");
    }

    @Test
    void findBySimpleProperty_AND() {
        QueryParam firstName = of(leroi2.getFirstName());
        QueryParam age = of(leroi2.getAge());
        List<Person> persons = repository.findByFirstNameAndAge(firstName, age);
        assertThat(persons).containsOnly(leroi2);
    }

    @Test
    void findBySimpleProperty_OR() {
        QueryParam firstName = of(carter.getFirstName());
        QueryParam age = of(leroi2.getAge()); // leroi2 and douglas have the same age
        List<Person> persons = repository.findByFirstNameOrAge(firstName, age);
        assertThat(persons).containsOnly(carter, leroi2, douglas);
    }

    @Test
    void findBySimpleProperty_AND_AND() {
        QueryParam firstName = of(leroi2.getFirstName());
        QueryParam age = of(leroi2.getAge());
        QueryParam lastName = of(leroi2.getLastName());
        List<Person> persons = repository.findByFirstNameAndAgeAndLastName(firstName, age, lastName);
        assertThat(persons).containsOnly(leroi2);
    }

    @Test
    void findBySimpleProperty_AND_OR() {
        QueryParam firstName = of(leroi2.getFirstName());
        QueryParam age = of(leroi2.getAge());
        QueryParam lastName = of(carter.getLastName());
        // The query is divided by OrParts by Spring Data Commons,
        // with OR combination being the upper level: OR(AND(firstName, age), lastName)
        List<Person> persons = repository.findByFirstNameAndAgeOrLastName(firstName, age, lastName);
        assertThat(persons).containsExactlyInAnyOrder(leroi2, carter);
    }

    @Test
    void findBySimpleProperty_OR_AND() {
        QueryParam firstName = of(leroi2.getFirstName());
        QueryParam age = of(carter.getAge());
        QueryParam lastName = of(carter.getLastName());
        // The query is divided by OrParts by Spring Data Commons,
        // with OR combination being the upper level: OR(firstName, AND(age, lastName))
        List<Person> persons = repository.findByFirstNameOrAgeAndLastName(firstName, age, lastName);
        assertThat(persons).containsExactlyInAnyOrder(leroi, leroi2, carter);
    }

    @Test
    void findBySimpleProperty_OR_OR() {
        QueryParam firstName = of(leroi2.getFirstName());
        QueryParam age = of(douglas.getAge());
        QueryParam lastName = of(carter.getLastName());
        List<Person> persons = repository.findByFirstNameOrAgeOrLastName(firstName, age, lastName);
        assertThat(persons).containsExactlyInAnyOrder(leroi, leroi2, douglas, carter);
    }
}
