/*
 * Copyright 2019 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.core;

import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.CollectionUtils;
import org.springframework.data.aerospike.QueryUtils;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.domain.Sort;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AerospikeTemplateFindByQueryTests extends BaseBlockingIntegrationTests {

    Person jean = Person.builder().id(nextId()).firstName("Jean").lastName("Matthews").age(21).ints(Collections.singletonList(100))
            .strings(Collections.singletonList("str1")).friend(new Person("id21", "TestPerson21", 50)).build();
    Person ashley = Person.builder().id(nextId()).firstName("Ashley").lastName("Matthews").ints(Collections.singletonList(22))
            .strings(Collections.singletonList("str2")).age(22).friend(new Person("id22", "TestPerson22", 50)).build();
    Person beatrice = Person.builder().id(nextId()).firstName("Beatrice").lastName("Matthews").age(23).ints(Collections.singletonList(23))
            .friend(new Person("id23", "TestPerson23", 42)).build();
    Person dave = Person.builder().id(nextId()).firstName("Dave").lastName("Matthews").age(24).stringMap(Collections.singletonMap("key1", "val1"))
            .friend(new Person("id21", "TestPerson24", 54)).build();
    Person zaipper = Person.builder().id(nextId()).firstName("Zaipper").lastName("Matthews").age(25)
            .stringMap(Collections.singletonMap("key2", "val2")).address(new Address("Street 1", "C0121", "Sun City")).build();
    Person knowlen = Person.builder().id(nextId()).firstName("knowlen").lastName("Matthews").age(26)
            .intMap(Collections.singletonMap("key1", 11)).address(new Address("Street 2", "C0122", "Sun City")).build();
    Person xylophone = Person.builder().id(nextId()).firstName("Xylophone").lastName("Matthews").age(27)
            .intMap(Collections.singletonMap("key2", 22)).address(new Address("Street 3", "C0123", "Sun City")).build();
    Person mitch = Person.builder().id(nextId()).firstName("Mitch").lastName("Matthews").age(28)
            .intMap(Collections.singletonMap("key3", 24)).address(new Address("Street 4", "C0124", "Sun City")).build();
    Person alister = Person.builder().id(nextId()).firstName("Alister").lastName("Matthews").age(29)
            .stringMap(Collections.singletonMap("key4", "val4")).build();
    Person aabbot = Person.builder().id(nextId()).firstName("Aabbot").lastName("Matthews").age(30)
            .stringMap(Collections.singletonMap("key4", "val5")).build();
    List<Person> all = Arrays.asList(jean, ashley, beatrice, dave, zaipper, knowlen, xylophone, mitch, alister, aabbot);

    @BeforeAll
    public void beforeAllSetUp() {
        additionalAerospikeTestOperations.deleteAllAndVerify(Person.class);

        template.insertAll(all);

        additionalAerospikeTestOperations.createIndexIfNotExists(Person.class, "person_age_index", "age", IndexType.NUMERIC);
        additionalAerospikeTestOperations.createIndexIfNotExists(Person.class, "person_first_name_index", "firstName", IndexType.STRING);
        additionalAerospikeTestOperations.createIndexIfNotExists(Person.class, "person_last_name_index", "lastName", IndexType.STRING);
    }

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
    }

    @Test
    public void findWithFilterEqual() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findPersonByFirstName", "Dave");

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result).containsOnly(dave);
    }

    @Test
    public void findWithFilterEqualOrderByAsc() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByLastNameOrderByFirstNameAsc", "Matthews");

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(10)
                .containsExactly(aabbot, alister, ashley, beatrice, dave, jean, knowlen, mitch, xylophone, zaipper);
    }

    @Test
    public void findWithFilterEqualOrderByDesc() {
        Object[] args = {"Matthews"};
        Query query = QueryUtils.createQueryForMethodWithArgs("findByLastNameOrderByFirstNameDesc", args);

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(10)
                .containsExactly(zaipper, xylophone, mitch, knowlen, jean, dave, beatrice, ashley, alister, aabbot);
    }

    @Test
    public void findWithFilterEqualOrderByDescNonExisting() {
        Object[] args = {"NonExistingSurname"};
        Query query = QueryUtils.createQueryForMethodWithArgs("findByLastNameOrderByFirstNameDesc", args);

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result).isEmpty();
    }

    @Test
    public void findWithFilterRange() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findCustomerByAgeBetween", 25, 30);

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(6);
    }

    @Test
    public void findWithFilterRangeNonExisting() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findCustomerByAgeBetween", 100, 150);

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result).isEmpty();
    }

    @Test
    public void findWithStatement() {
        Statement aerospikeQuery = new Statement();
        String[] bins = {"firstName", "lastName"}; //fields we want retrieved
        aerospikeQuery.setNamespace(getNameSpace());
        aerospikeQuery.setSetName("Person");
        aerospikeQuery.setBinNames(bins);
        aerospikeQuery.setFilter(Filter.equal("firstName", dave.getFirstName()));

        RecordSet rs = client.query(null, aerospikeQuery);

        assertThat(CollectionUtils.toList(rs))
                .singleElement()
                .satisfies(record ->
                        assertThat(record.bins)
                                .containsOnly(entry("firstName", dave.getFirstName()), entry("lastName", dave.getLastName())));
    }

    @Test
    public void findInRange_shouldFindLimitedNumberOfDocuments() {
        int skip = 0;
        int limit = 5;
        Stream<Person> stream = template.findInRange(skip, limit, Sort.unsorted(), Person.class);

        assertThat(stream)
                .hasSize(5);
    }

    @Test
    public void findInRange_shouldFindLimitedNumberOfDocumentsAndSkip() {
        int skip = 3;
        int limit = 5;
        Stream<Person> stream = template.findInRange(skip, limit, Sort.unsorted(), Person.class);

        assertThat(stream)
                .hasSize(5);
    }

    @Test
    public void findAll_findAllExistingDocuments() {
        Stream<Person> result = template.findAll(Person.class);

        assertThat(result).containsAll(all);
    }

    @Test
    public void findAll_findNothing() {
        additionalAerospikeTestOperations.deleteAllAndVerify(Person.class);

        Stream<Person> result = template.findAll(Person.class);

        assertThat(result).isEmpty();

        template.insertAll(all);
    }

    @Test
    public void find_throwsExceptionForUnsortedQueryWithSpecifiedOffsetValue() {
        Query query = new Query((Sort) null);
        query.setOffset(1);

        assertThatThrownBy(() -> template.find(query, Person.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsorted query must not have offset value. For retrieving paged results use sorted query.");
    }


    @Test
    public void findByListContainingInteger() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByIntsContaining", 100);

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrder(jean);
    }

    @Test
    public void findByListContainingString() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByStringsContaining", "str2");

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrder(ashley);
    }

    @Test
    public void findByListValueLessThanOrEqual() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByIntsLessThanEqual", 25);

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(2)
                .containsExactlyInAnyOrder(ashley, beatrice);
    }

    @Test
    public void findByListValueGreaterThan() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByIntsGreaterThan", 10);

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(3)
                .containsExactlyInAnyOrder(ashley, beatrice, jean);
    }

    @Test
    public void findByListValueInRange() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByIntsBetween", 10, 700);

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(3)
                .containsExactlyInAnyOrder(jean, ashley, beatrice);
    }

    @Test
    public void findByMapKeysContaining() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByStringMapContaining", "key1", CriteriaDefinition.AerospikeMapCriteria.KEY);

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrder(dave);
    }

    @Test
    public void findByMapValuesContaining() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByStringMapContaining", "val2", CriteriaDefinition.AerospikeMapCriteria.VALUE);

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrder(zaipper);
    }

    @Test
    public void findByMapKeyValueEquals() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByStringMapEquals", "key1", "val1");

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrder(dave);
    }

    @Test
    public void findByMapKeyValueNotEquals() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByIntMapIsNot", "key2", 11);

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrder(xylophone);
    }

    @Test
    public void findByMapKeyValueGreaterThan() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByIntMapGreaterThan", "key1", 1);

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrder(knowlen);
    }

    @Test
    public void findByMapKeyValueBetween() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByIntMapBetween", "key3", 11, 24);

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrder(mitch);
    }

    @Test
    public void findByMapKeyValueStartsWith() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByStringMapStartsWith", "key4", "val");

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(2)
                .containsExactlyInAnyOrder(alister, aabbot);
    }

    @Test
    public void findByMapKeyValueContains() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByStringMapContaining", "key4", "al");

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(2)
                .containsExactlyInAnyOrder(alister, aabbot);
    }

    @Test
    public void findPersonsByFriendAge() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByFriendAge", 50);

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(2)
                .containsExactlyInAnyOrder(jean, ashley);
    }

    @Test
    public void findPersonsByFriendAgeNotEqual() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByFriendAgeIsNot", 50);

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(2)
                .containsExactlyInAnyOrder(beatrice, dave);
    }

    @Test
    public void findPersonsByFriendAgeGreaterThan() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByFriendAgeGreaterThan", 42);

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(3)
                .containsExactlyInAnyOrder(jean, ashley, dave);
    }

    @Test
    public void findPersonsByFriendAgeLessThanOrEqual() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByFriendAgeLessThanEqual", 54);

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(4)
                .containsExactlyInAnyOrder(jean, ashley, beatrice, dave);
    }

    @Test
    public void findPersonsByFriendAgeRange() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByFriendAgeBetween", 50, 54);

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(3)
                .containsExactlyInAnyOrder(jean, ashley, dave);
    }

    @Test
    public void findPersonsByAddressZipCode() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByAddressZipCode", "C0123");

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrder(xylophone);
    }

    @Test
    public void findByAddressZipCodeContaining() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByAddressZipCodeContaining", "C012");

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(4)
                .containsExactlyInAnyOrder(zaipper, knowlen, xylophone, mitch);
    }
}
