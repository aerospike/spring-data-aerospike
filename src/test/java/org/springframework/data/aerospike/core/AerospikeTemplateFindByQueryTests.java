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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.CollectionUtils;
import org.springframework.data.aerospike.QueryUtils;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.domain.Sort;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

public class AerospikeTemplateFindByQueryTests extends BaseBlockingIntegrationTests {

    Person jean = Person.builder().id(nextId()).firstName("Jean").lastName("Matthews").age(21).build();
    Person ashley = Person.builder().id(nextId()).firstName("Ashley").lastName("Matthews").age(22).build();
    Person beatrice = Person.builder().id(nextId()).firstName("Beatrice").lastName("Matthews").age(23).build();
    Person dave = Person.builder().id(nextId()).firstName("Dave").lastName("Matthews").age(24).build();
    Person zaipper = Person.builder().id(nextId()).firstName("Zaipper").lastName("Matthews").age(25).build();
    Person knowlen = Person.builder().id(nextId()).firstName("knowlen").lastName("Matthews").age(26).build();
    Person xylophone = Person.builder().id(nextId()).firstName("Xylophone").lastName("Matthews").age(27).build();
    Person mitch = Person.builder().id(nextId()).firstName("Mitch").lastName("Matthews").age(28).build();
    Person alister = Person.builder().id(nextId()).firstName("Alister").lastName("Matthews").age(29).build();
    Person aabbot = Person.builder().id(nextId()).firstName("Aabbot").lastName("Matthews").age(30).build();
    List<Person> all = Arrays.asList(jean, ashley, beatrice, dave, zaipper, knowlen, xylophone, mitch, alister, aabbot);

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
        blockingAerospikeTestOperations.deleteAll(Person.class);

        template.insertAll(all);

        blockingAerospikeTestOperations.createIndexIfNotExists(Person.class, "age_index", "age", IndexType.NUMERIC);
        blockingAerospikeTestOperations.createIndexIfNotExists(Person.class, "first_name_index", "firstName", IndexType.STRING);
        blockingAerospikeTestOperations.createIndexIfNotExists(Person.class, "last_name_index", "lastName", IndexType.STRING);
    }

    @Test
    public void testFindWithFilterEqual() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findPersonByFirstName", "Dave");

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result).containsOnly(dave);
    }

    @Test
    public void testFindWithFilterEqualOrderBy() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findByLastNameOrderByFirstNameAsc", "Matthews");

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(10)
                .containsExactly(aabbot, alister, ashley, beatrice, dave, jean, knowlen, mitch, xylophone, zaipper);
    }

    @Test
    public void testFindWithFilterEqualOrderByDesc() {
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
    public void testFindWithFilterRange() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findCustomerByAgeBetween", 25, 30);

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result)
                .hasSize(6);
    }

    @Test
    public void testFindWithFilterRangeNonExisting() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findCustomerByAgeBetween", 100, 150);

        Stream<Person> result = template.find(query, Person.class);

        assertThat(result).isEmpty();
    }

    @Test
    public void testFindWithStatement() {
        Statement aerospikeQuery = new Statement();
        String[] bins = {"firstName", "lastName"}; //fields we want retrieved
        aerospikeQuery.setNamespace(getNameSpace());
        aerospikeQuery.setSetName("Person");
        aerospikeQuery.setBinNames(bins);
        aerospikeQuery.setFilter(Filter.equal("firstName", dave.getFirstName()));

        RecordSet rs = client.query(null, aerospikeQuery);

        assertThat(CollectionUtils.toList(rs))
                .hasOnlyOneElementSatisfying(record ->
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
    public void findAll_findsAllExistingDocuments() {
        Stream<Person> result = template.findAll(Person.class);

        assertThat(result).containsAll(all);
    }

    @Test
    public void findAll_findsNothing() {
        blockingAerospikeTestOperations.deleteAll(Person.class);

        Stream<Person> result = template.findAll(Person.class);

        assertThat(result).isEmpty();
    }

    @Test
    public void find_throwsExceptionForUnsortedQueryWithSpecifiedOffsetValue() {
        Query query = new Query((Sort) null);
        query.setOffset(1);

        assertThatThrownBy(() -> template.find(query, Person.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsorted query must not have offset value. For retrieving paged results use sorted query.");
    }
}
