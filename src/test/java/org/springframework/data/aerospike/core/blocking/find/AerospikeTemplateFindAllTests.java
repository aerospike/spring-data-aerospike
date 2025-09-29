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
package org.springframework.data.aerospike.core.blocking.find;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.data.aerospike.annotation.Nightly;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.SampleClasses;
import org.springframework.data.domain.Sort;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.domain.Sort.Order.asc;

@Nightly
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AerospikeTemplateFindAllTests extends AerospikeTemplateFindByQueryTests {

    @Test
    public void findAll_OrderByFirstName() {
        Sort sort = Sort.by(asc("firstName"));
        List<Person> result = template.findAll(sort, 0, 0, Person.class)
            .collect(Collectors.toList());

        assertThat(result)
            .hasSize(10)
            .containsExactly(aabbot, alister, ashley, beatrice, dave, jean, knowlen, mitch, xylophone, zaipper);
    }

    @Test
    public void findAll_OrderByFirstNameWithSetName() {
        Sort sort = Sort.by(asc("firstName"));
        List<Person> result = template.findAll(sort, 0, 0, Person.class, OVERRIDE_SET_NAME)
            .collect(Collectors.toList());

        assertThat(result)
            .hasSize(10)
            .containsExactly(aabbot, alister, ashley, beatrice, dave, jean, knowlen, mitch, xylophone, zaipper);
    }

    @Test
    public void findAll_findAllExistingDocuments() {
        Stream<Person> result = template.findAll(Person.class);
        assertThat(result).containsAll(allPersons);
    }

    @Test
    public void findAll_findNothing() {
        additionalAerospikeTestOperations.deleteAllAndVerify(Person.class);

        Stream<Person> result = template.findAll(Person.class);
        assertThat(result).isEmpty();

        // bring records back
        template.insertAll(allPersons);
    }

    @Test
    public void findAll_findIdOnlyRecord() {
        var id = 100;
        var doc = new SampleClasses.DocumentWithPrimitiveIntId(id); // id-only document
        var clazz = SampleClasses.DocumentWithPrimitiveIntId.class;

        var existingDoc = template.findById(id, clazz);
        assertThat(existingDoc).withFailMessage("The same record already exists").isNull();

        template.insert(doc);
        var resultsFindById = template.findById(id, clazz);
        assertThat(resultsFindById).withFailMessage("findById error").isEqualTo(doc);
        var resultsFindAll = template.findAll(clazz);
        // findAll() must correctly find the record that contains id and no bins
        assertThat(resultsFindAll.toList()).size().withFailMessage("findAll error").isEqualTo(1);

        // cleanup
        template.delete(doc);
    }
}
