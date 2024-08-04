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
package org.springframework.data.aerospike.transaction.sync;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.SampleClasses;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AerospikeTemplateInsertInTransactionTests extends BaseBlockingIntegrationTests {

    @Autowired
    AerospikeTransactionManager transactionManager;

    @Autowired
    TransactionTemplate transactionTemplate;

    AerospikeTransactionManager mockTxManager = mock(AerospikeTransactionManager.class);
    TransactionTemplate mockTxTemplate = new TransactionTemplate(mockTxManager);

    @BeforeEach
    public void beforeEach() {
        deleteAll(Person.class, SampleClasses.DocumentWithPrimitiveIntId.class,
            SampleClasses.DocumentWithIntegerId.class);
    }

    @AfterEach
    void verifyTransactionResourcesReleased() {
        assertThat(TransactionSynchronizationManager.getResourceMap().isEmpty()).isTrue();
        assertThat(TransactionSynchronizationManager.isSynchronizationActive()).isFalse();
    }

    @AfterAll
    public void afterAll() {
        deleteAll(Person.class, SampleClasses.DocumentWithPrimitiveIntId.class,
            SampleClasses.DocumentWithIntegerId.class);
    }

    @Test
    public void insertInTransaction_verifyCommit_unitTest() {
        mockTxTemplate.executeWithoutResult(status -> {
            template.insert(new SampleClasses.DocumentWithPrimitiveIntId(100));
        });

        // verify that commit() was called
        verify(mockTxManager).commit(null);

        // resource holder must be already released
        assertThat(TransactionSynchronizationManager.getResource(client)).isNull();
    }

    @Test
    public void insertInTransaction_multipleWrites() {
        // Multi-record transactions are supported starting with Server version 8.0+
        transactionTemplate.executeWithoutResult(status -> {
            assertThat(status.isNewTransaction()).isTrue();
            template.insert(new SampleClasses.DocumentWithIntegerId(100, "test1"));
            template.save(new SampleClasses.DocumentWithIntegerId(100, "test2"));
        });

        var result = template.findById(100, SampleClasses.DocumentWithIntegerId.class);
        assertThat(result.getContent().equals("test2")).isTrue();
    }

    @Test
    public void insertAllInTransaction_insertsAllDocuments() {
        // Multi-record transactions are supported starting with Server version 8.0+
//        if (serverVersionSupport.isMRTSupported()) { // TODO: uncomment when server 8 is released, maybe as annotation
        List<Person> persons = IntStream.range(1, 10)
            .mapToObj(age -> Person.builder().id(nextId())
                .firstName("Gregor")
                .age(age).build())
            .collect(Collectors.toList());

        transactionTemplate.executeWithoutResult(status -> {
            assertThat(status.isNewTransaction()).isTrue();
            template.insertAll(persons);
        });

        var results = template.findAll(Person.class);
        assertThat(results.toList()).containsAll(persons);
//        }
    }

    @Test
    @Transactional
    public void test() {
        template.insertAll(List.of(new SampleClasses.DocumentWithPrimitiveIntId(100))); // TODO: test
    }

    @Test
    public void insertInTransaction_rollbackWorks() {
        template.insert(new SampleClasses.DocumentWithPrimitiveIntId(100));

        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status ->
            template.insert(new SampleClasses.DocumentWithPrimitiveIntId(100))))
            .isInstanceOf(DuplicateKeyException.class)
            .hasMessageContaining("Key already exists");

//        var results = template.findAll(SampleClasses.DocumentWithPrimitiveIntId.class); // TODO: check
//        assertThat(results.toList()).size().isEqualTo(1);
        var result = template.findById(100, SampleClasses.DocumentWithPrimitiveIntId.class);
        assertThat(result.getId()).isEqualTo(100);
    }

    @Test
    public void multipleWritesInTransaction_rollbackWorks() {
        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
                template.insert(new SampleClasses.DocumentWithPrimitiveIntId(100));
                template.insert(new SampleClasses.DocumentWithPrimitiveIntId(100));
            }
            ))
            .isInstanceOf(DuplicateKeyException.class)
            .hasMessageContaining("Key already exists");

        // No record was written because all writes were in the same transaction
        assertThat(template.findById(100, SampleClasses.DocumentWithPrimitiveIntId.class)).isNull();
    }

    @Test
    @Transactional
    public void test2() { // TODO: direct calls to client within a transaction
        WritePolicy wp = client.copyWritePolicyDefault();
        wp.expiration = 1;
        // some specific configuration
        Key key = new Key("TEST", "testSet", "newKey1");
        client.put(wp, key, new Bin("bin1", "val1"));

        WritePolicy wp2 = client.copyWritePolicyDefault();
        wp.durableDelete = true;
        // some specific configuration
        Key key2 = new Key("TEST", "testSet", "existingKey2");
        client.delete(wp2, key2);
    }
}
