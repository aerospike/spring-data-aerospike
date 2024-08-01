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

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.SampleClasses;
import org.springframework.data.aerospike.sample.SampleClasses.CustomCollectionClass;
import org.springframework.data.aerospike.sample.SampleClasses.DocumentWithByteArray;
import org.springframework.data.aerospike.transaction.sync.AerospikeTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.springframework.data.aerospike.sample.SampleClasses.VersionedClass;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AerospikeTemplateInsertInTransactionTests extends BaseBlockingIntegrationTests {

    @Autowired
    AerospikeTransactionManager transactionManager;

    @Autowired
    TransactionTemplate transactionTemplate;

    @BeforeEach
    public void beforeEach() {
        deleteAll(Person.class, CustomCollectionClass.class, DocumentWithByteArray.class, VersionedClass.class,
            OVERRIDE_SET_NAME);
    }

    @AfterAll
    public void afterAll() {
        deleteAll(Person.class, CustomCollectionClass.class, DocumentWithByteArray.class, VersionedClass.class,
            OVERRIDE_SET_NAME);
    }

    @Test
    public void insertInTransaction_verifyCommit_unitTest() {
        AerospikeTransactionManager mockTxManager = mock(AerospikeTransactionManager.class);
        TransactionTemplate mockTxTemplate = new TransactionTemplate(mockTxManager);

        mockTxTemplate.executeWithoutResult(status -> {
            template.insert(new SampleClasses.DocumentWithPrimitiveIntId(100));
        });

        // verify that commit() has been called
        verify(mockTxManager).commit(null);
    }

    @Test
    public void insertAllInTransaction_insertsAllDocuments() {
        // Multi-record transactions are supported starting with Server version 8.0+
        if (serverVersionSupport.isMRTSupported()) {
            List<Person> persons = IntStream.range(1, 10)
                .mapToObj(age -> Person.builder().id(nextId())
                    .firstName("Gregor")
                    .age(age).build())
                .collect(Collectors.toList());

            transactionTemplate.executeWithoutResult(status -> {
                template.insertAll(persons);
//                assertThat(status.isNewTransaction()).isTrue();
            });
        }
    }

    @Test
    @Transactional
    public void test() {
        template.insertAll(null); // TODO
    }

    @Test
    @Transactional
    public void test2() { // TODO
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
