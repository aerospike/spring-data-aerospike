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
package org.springframework.data.aerospike.core.sync;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.domain.Sort;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.domain.Sort.Order.asc;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AerospikeTemplateFindInRangeTests extends AerospikeTemplateFindByQueryTests {

    @Test
    public void findInRange_shouldFindLimitedNumberOfDocuments() {
        int skip = 0;
        int limit = 5;
        Stream<Person> stream = template.findInRange(skip, limit, Sort.unsorted(), Person.class);
        assertThat(stream).hasSize(5);
    }

    @Test
    public void findInRangeWithSetName_shouldFindLimitedNumberOfDocuments() {
        int skip = 0;
        int limit = 5;
        Stream<Person> stream = template.findInRange(skip, limit, Sort.unsorted(), Person.class, OVERRIDE_SET_NAME);
        assertThat(stream).hasSize(5);
    }

    @Test
    public void findInRange_shouldFailOnUnsortedQueryWithOffsetValue() {
        int skip = 3;
        int limit = 5;
        assertThatThrownBy(() -> template.findInRange(skip, limit, Sort.unsorted(), Person.class))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unsorted query must not have offset value. For retrieving paged results use sorted query.");
    }

    @Test
    public void findInRangeWithSetName_shouldFailOnUnsortedQueryWithOffsetValue() {
        int skip = 3;
        int limit = 5;
        assertThatThrownBy(() -> template.findInRange(skip, limit, Sort.unsorted(), Person.class, OVERRIDE_SET_NAME))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unsorted query must not have offset value. For retrieving paged results use sorted query.");
    }

    @Test
    public void findInRange_shouldFindLimitedNumberOfDocumentsWithOrderBy() {
        int skip = 0;
        int limit = 5;
        Sort sort = Sort.by(asc("firstName"));
        List<Person> stream = template.findInRange(skip, limit, sort, Person.class)
            .collect(Collectors.toList());

        assertThat(stream)
            .hasSize(5)
            .containsExactly(aabbot, alister, ashley, beatrice, dave);
    }
}
