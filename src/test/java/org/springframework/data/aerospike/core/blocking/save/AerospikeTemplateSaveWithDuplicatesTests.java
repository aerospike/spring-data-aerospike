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
package org.springframework.data.aerospike.core.blocking.save;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.SampleClasses;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.sample.SampleClasses.CustomCollectionClass;
import static org.springframework.data.aerospike.sample.SampleClasses.DocumentWithTouchOnRead;
import static org.springframework.data.aerospike.sample.SampleClasses.VersionedClass;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AerospikeTemplateSaveWithDuplicatesTests extends BaseBlockingIntegrationTests {

    @AfterAll
    public void afterAll() {
        template.deleteAll(VersionedClass.class);
        template.deleteAll(SampleClasses.DocumentWithIntArray.class);
        template.deleteAll(SampleClasses.DocumentWithBigIntegerAndNestedArray.class);
        template.deleteAll(Person.class);
        template.deleteAll(CustomCollectionClass.class);
        template.deleteAll(DocumentWithTouchOnRead.class);
        template.deleteAll(OVERRIDE_SET_NAME);
    }

    @Test
    public void shouldSaveAllVersionedDocumentsAndSetVersionAndThrowExceptionIfDuplicatesWithinOneBatch() {
        VersionedClass first = new VersionedClass("newId100", "foo");
        VersionedClass second = new VersionedClass("newId200", "foo");

        // The documents’ versions are equal to zero, meaning the documents have not been saved to the database yet
        assertThat(first.getVersion()).isSameAs(0);
        assertThat(second.getVersion()).isSameAs(0);

        // An attempt to save the same versioned documents in one batch results in getting an exception
        assertThatThrownBy(() -> template.saveAll(List.of(first, first, second, second)))
            .isInstanceOf(OptimisticLockingFailureException.class)
            .hasMessageFindingMatch("Failed to save the record with ID .* due to versions mismatch");

        // The documents' versions get updated after they are read from the corresponding database records
        assertThat(first.getVersion()).isSameAs(1);
        assertThat(second.getVersion()).isSameAs(1);

        template.delete(first); // cleanup
        template.delete(second); // cleanup
    }

    @Test
    public void shouldSaveAllVersionedDocumentsIfDuplicatesNotWithinOneBatch() {
        // The same versioned documents can be saved if they are not in the same batch.
        // This way, the generation counts of the corresponding database records can be used
        // to update the documents’ versions each time.
        VersionedClass newFirst = new VersionedClass("newId300", "foo");
        VersionedClass newSecond = new VersionedClass("newId400", "bar");

        assertThat(newFirst.getVersion()).isSameAs(0);
        assertThat(newSecond.getVersion()).isSameAs(0);

        template.saveAll(List.of(newFirst, newSecond));
        // Failed to save the record with ID 'newId2' due to versions mismatch
        assertThat(newFirst.getVersion()).isSameAs(1);
        assertThat(newSecond.getVersion()).isSameAs(1);

        template.saveAll(List.of(newFirst, newSecond));
        assertThat(newFirst.getVersion()).isSameAs(2);
        assertThat(newSecond.getVersion()).isSameAs(2);
    }
}
