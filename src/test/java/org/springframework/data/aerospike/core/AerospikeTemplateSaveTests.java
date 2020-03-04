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

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import org.junit.Test;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.aerospike.AsyncUtils;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.sample.Person;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.SampleClasses.CustomCollectionClass;
import static org.springframework.data.aerospike.SampleClasses.DocumentWithByteArray;
import static org.springframework.data.aerospike.SampleClasses.DocumentWithTouchOnRead;
import static org.springframework.data.aerospike.SampleClasses.VersionedClass;

public class AerospikeTemplateSaveTests extends BaseBlockingIntegrationTests {

    //test for RecordExistsAction.REPLACE_ONLY policy
    @Test
    public void shouldReplaceAllBinsPresentInAerospikeWhenSavingDocument() {
        Key key = new Key(getNameSpace(), "versioned-set", id);
        VersionedClass first = new VersionedClass(id, "foo");
        template.save(first);
        blockingAerospikeTestOperations.addNewFieldToSavedDataInAerospike(key);

        template.save(new VersionedClass(id, "foo2", 2L));

        Record record2 = client.get(new Policy(), key);
        assertThat(record2.bins.get("notPresent")).isNull();
        assertThat(record2.bins.get("field")).isEqualTo("foo2");
    }

    @Test
    public void shouldSaveAndSetVersion() {
        VersionedClass first = new VersionedClass(id, "foo");
        template.save(first);

        assertThat(first.version).isEqualTo(1);
        assertThat(template.findById(id, VersionedClass.class).version).isEqualTo(1);
    }

    @Test
    public void shouldNotSaveDocumentIfItAlreadyExists() {
        template.save(new VersionedClass(id, "foo"));

        assertThatThrownBy(() -> template.save(new VersionedClass(id, "foo")))
                .isInstanceOf(OptimisticLockingFailureException.class);
    }

    @Test
    public void shouldSaveDocumentWithEqualVersion() {
        template.save(new VersionedClass(id, "foo", 0L));

        template.save(new VersionedClass(id, "foo", 1L));
        template.save(new VersionedClass(id, "foo", 2L));
    }

    @Test
    public void shouldFailSaveNewDocumentWithVersionGreaterThanZero() {
        assertThatThrownBy(() -> template.save(new VersionedClass(id, "foo", 5L)))
                .isInstanceOf(DataRetrievalFailureException.class);
    }

    @Test
    public void shouldUpdateNullField() {
        VersionedClass versionedClass = new VersionedClass(id, null);
        template.save(versionedClass);

        VersionedClass saved = template.findById(id, VersionedClass.class);
        template.save(saved);
    }

    @Test
    public void shouldUpdateNullFieldForClassWithVersionField() {
        VersionedClass versionedClass = new VersionedClass(id, "field");
        template.save(versionedClass);

        VersionedClass byId = template.findById(id, VersionedClass.class);
        assertThat(byId.getField())
                .isEqualTo("field");

        template.save(new VersionedClass(id, null, byId.version));

        assertThat(template.findById(id, VersionedClass.class).getField())
                .isNull();
    }

    @Test
    public void shouldUpdateNullFieldForClassWithoutVersionField() {
        Person person = new Person(id, "Oliver");
        person.setFirstName("First name");
        template.insert(person);

        assertThat(template.findById(id, Person.class)).isEqualTo(person);

        person.setFirstName(null);
        template.save(person);

        assertThat(template.findById(id, Person.class).getFirstName()).isNull();
    }

    @Test
    public void shouldUpdateExistingDocument() {
        VersionedClass one = new VersionedClass(id, "foo");
        template.save(one);

        template.save(new VersionedClass(id, "foo1", one.version));

        VersionedClass value = template.findById(id, VersionedClass.class);
        assertThat(value.version).isEqualTo(2);
        assertThat(value.field).isEqualTo("foo1");
    }

    @Test
    public void shouldSetVersionWhenSavingTheSameDocument() {
        VersionedClass one = new VersionedClass(id, "foo");
        template.save(one);
        template.save(one);
        template.save(one);

        assertThat(one.version).isEqualTo(3);
    }

    @Test
    public void shouldUpdateAlreadyExistingDocument() throws Exception {
        AtomicLong counter = new AtomicLong();
        int numberOfConcurrentSaves = 5;

        VersionedClass initial = new VersionedClass(id, "value-0");
        template.save(initial);
        assertThat(initial.version).isEqualTo(1);

        AsyncUtils.executeConcurrently(numberOfConcurrentSaves, () -> {
            boolean saved = false;
            while (!saved) {
                long counterValue = counter.incrementAndGet();
                VersionedClass messageData = template.findById(id, VersionedClass.class);
                messageData.field = "value-" + counterValue;
                try {
                    template.save(messageData);
                    saved = true;
                } catch (OptimisticLockingFailureException e) {
                }
            }
        });

        VersionedClass actual = template.findById(id, VersionedClass.class);

        assertThat(actual.field).isNotEqualTo(initial.field);
        assertThat(actual.version).isNotEqualTo(initial.version);
        assertThat(actual.version).isEqualTo(initial.version + numberOfConcurrentSaves);
    }

    @Test
    public void shouldSaveOnlyFirstDocumentAndNextAttemptsShouldFailWithOptimisticLockingException() throws Exception {
        AtomicLong counter = new AtomicLong();
        AtomicLong optimisticLockCounter = new AtomicLong();
        int numberOfConcurrentSaves = 5;

        AsyncUtils.executeConcurrently(numberOfConcurrentSaves, () -> {
            long counterValue = counter.incrementAndGet();
            String data = "value-" + counterValue;
            VersionedClass messageData = new VersionedClass(id, data);
            try {
                template.save(messageData);
            } catch (OptimisticLockingFailureException e) {
                optimisticLockCounter.incrementAndGet();
            }
        });

        assertThat(optimisticLockCounter.intValue()).isEqualTo(numberOfConcurrentSaves - 1);
    }

    @Test
    public void shouldSaveMultipleTimeDocumentWithoutVersion() {
        CustomCollectionClass one = new CustomCollectionClass(id, "numbers");

        template.save(one);
        template.save(one);

        assertThat(template.findById(id, CustomCollectionClass.class)).isEqualTo(one);
    }

    @Test
    public void shouldUpdateDocumentDataWithoutVersion() {
        CustomCollectionClass first = new CustomCollectionClass(id, "numbers");
        CustomCollectionClass second = new CustomCollectionClass(id, "hot dog");

        template.save(first);
        template.save(second);

        assertThat(template.findById(id, CustomCollectionClass.class)).isEqualTo(second);
    }

    @Test
    public void rejectsNullObjectToBeSaved() {
        assertThatThrownBy(() -> template.save(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldConcurrentlyUpdateDocumentIfTouchOnReadIsTrue() throws Exception {
        int numberOfConcurrentUpdate = 10;
        AsyncUtils.executeConcurrently(numberOfConcurrentUpdate, new Runnable() {
            @Override
            public void run() {
                try {
                    DocumentWithTouchOnRead existing = template.findById(id, DocumentWithTouchOnRead.class);
                    DocumentWithTouchOnRead toUpdate;
                    if (existing != null) {
                        toUpdate = new DocumentWithTouchOnRead(id, existing.getField() + 1, existing.getVersion());
                    } else {
                        toUpdate = new DocumentWithTouchOnRead(id, 1);
                    }

                    template.save(toUpdate);
                } catch (ConcurrencyFailureException e) {
                    //try again
                    run();
                }
            }
        });

        DocumentWithTouchOnRead actual = template.findById(id, DocumentWithTouchOnRead.class);
        assertThat(actual.getField()).isEqualTo(numberOfConcurrentUpdate);
    }

    @Test
    public void shouldSaveAndFindDocumentWithByteArrayField() {
        DocumentWithByteArray document = new DocumentWithByteArray(id, new byte[]{1, 0, 0, 1, 1, 1, 0, 0});

        template.save(document);

        DocumentWithByteArray result = template.findById(id, DocumentWithByteArray.class);

        assertThat(result).isEqualTo(document);
    }

}
