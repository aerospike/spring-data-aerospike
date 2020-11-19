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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import org.assertj.core.data.Offset;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.SampleClasses.DocumentWithDefaultConstructor;
import org.springframework.data.aerospike.SampleClasses.DocumentWithExpiration;
import org.springframework.data.aerospike.SampleClasses.DocumentWithExpirationAnnotation;
import org.springframework.data.aerospike.SampleClasses.DocumentWithExpirationOneDay;
import org.springframework.data.aerospike.SampleClasses.DocumentWithUnixTimeExpiration;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.data.Offset.offset;
import static org.springframework.data.aerospike.SampleClasses.DocumentWithExpirationAnnotationAndPersistenceConstructor;
import static org.springframework.data.aerospike.utility.AerospikeExpirationPolicy.DO_NOT_UPDATE_EXPIRATION;
import static org.springframework.data.aerospike.utility.AerospikeExpirationPolicy.NEVER_EXPIRE;

//TODO: Potentially unstable tests. Instead of sleeping, we need somehow do time travel like in CouchbaseMock.
public class AerospikeExpirationTests extends BaseBlockingIntegrationTests {

    @AfterEach
    public void tearDown() {
        aerospikeTestOperations.rollbackTime();
    }

    @Test
    public void shouldAddValuesMapAndExpire() throws InterruptedException {
        DocumentWithDefaultConstructor document = new DocumentWithDefaultConstructor();
        document.setId(id);
        document.setExpiration(DateTime.now().plusSeconds(1));

        template.add(document, singletonMap("intField", 10L));
        Thread.sleep(500L);

        DocumentWithDefaultConstructor shouldNotExpire = template.findById(id, DocumentWithDefaultConstructor.class);
        assertThat(shouldNotExpire).isNotNull();
        assertThat(shouldNotExpire.getIntField()).isEqualTo(10);

        Thread.sleep(1500L);

        DocumentWithDefaultConstructor shouldExpire = template.findById(id, DocumentWithDefaultConstructor.class);
        assertThat(shouldExpire).isNull();
    }

    @Test
    public void shouldAddValueAndExpire() throws InterruptedException {
        DocumentWithDefaultConstructor document = new DocumentWithDefaultConstructor();
        document.setId(id);
        document.setExpiration(DateTime.now().plusSeconds(1));

        template.add(document, "intField", 10L);
        Thread.sleep(500L);

        DocumentWithDefaultConstructor shouldNotExpire = template.findById(id, DocumentWithDefaultConstructor.class);
        assertThat(shouldNotExpire).isNotNull();
        assertThat(shouldNotExpire.getIntField()).isEqualTo(10);

        Thread.sleep(1500L);

        DocumentWithDefaultConstructor shouldExpire = template.findById(id, DocumentWithDefaultConstructor.class);
        assertThat(shouldExpire).isNull();
    }

    @Test
    public void shouldExpireBasedOnUnixTimeValue() throws InterruptedException {
        template.insert(new DocumentWithUnixTimeExpiration(id, DateTime.now().plusSeconds(1)));

        Thread.sleep(500L);

        DocumentWithUnixTimeExpiration shouldNotExpire = template.findById(id, DocumentWithUnixTimeExpiration.class);
        assertThat(shouldNotExpire).isNotNull();

        Thread.sleep(1500L);

        DocumentWithUnixTimeExpiration shouldExpire = template.findById(id, DocumentWithUnixTimeExpiration.class);
        assertThat(shouldExpire).isNull();
    }

    @Test
    public void shouldExpireBasedOnFieldValue() throws InterruptedException {
        template.insert(new DocumentWithExpirationAnnotation(id, 1));

        Thread.sleep(500L);

        DocumentWithExpirationAnnotation shouldNotExpire = template.findById(id, DocumentWithExpirationAnnotation.class);
        assertThat(shouldNotExpire).isNotNull();

        Thread.sleep(1500L);

        DocumentWithExpirationAnnotation shouldExpire = template.findById(id, DocumentWithExpirationAnnotation.class);
        assertThat(shouldExpire).isNull();
    }

    @Test
    public void shouldReturnExpirationValue() throws InterruptedException {
        template.insert(new DocumentWithExpirationAnnotation(id, 5));

        Thread.sleep(1500L);

        DocumentWithExpirationAnnotation document = template.findById(id, DocumentWithExpirationAnnotation.class);
        assertThat(document.getExpiration()).isGreaterThan(0).isLessThan(5);
    }

    @Test
    public void shouldUpdateExpirationOnTouchOnRead() throws InterruptedException {
        template.insert(new DocumentWithExpirationOneDay(id));

        Key key = new Key(template.getNamespace(), template.getSetName(DocumentWithExpirationOneDay.class), id);

        Record record = template.getAerospikeClient().get(null, key);
        int initialExpiration = record.expiration;

        Thread.sleep(2_000);
        template.findById(id, DocumentWithExpirationOneDay.class);

        record = template.getAerospikeClient().get(null, key);
        assertThat(record.expiration - initialExpiration)
                .isCloseTo(2, offset(1));
    }

    @Test
    public void shouldExpire() throws Exception {
        template.insert(new DocumentWithExpiration(id));

        Thread.sleep(500L);

        DocumentWithExpiration shouldNotExpire = template.findById(id, DocumentWithExpiration.class);
        assertThat(shouldNotExpire).isNotNull();

        Thread.sleep(1500L);

        DocumentWithExpiration shouldExpire = template.findById(id, DocumentWithExpiration.class);
        assertThat(shouldExpire).isNull();
    }

    @Test
    public void shouldSaveAndGetDocumentWithImmutableExpiration() {
        template.insert(new DocumentWithExpirationAnnotationAndPersistenceConstructor(id, 60L));

        DocumentWithExpirationAnnotationAndPersistenceConstructor doc = template.findById(id, DocumentWithExpirationAnnotationAndPersistenceConstructor.class);
        assertThat(doc).isNotNull();
        assertThat(doc.getExpiration()).isCloseTo(60L, Offset.offset(10L));
    }

    @Test
    public void save_expiresDocumentWithVersion() {
        template.save(new DocumentWithExpirationOneDay(id));

        aerospikeTestOperations.addDuration(Duration.standardHours(24).plus(Duration.standardMinutes(1)));

        DocumentWithExpirationOneDay document = template.findById(id, DocumentWithExpirationOneDay.class);
        assertThat(document).isNull();
    }

    @Test
    void shouldNeverExpire() {
        DocumentWithExpirationAnnotation document = new DocumentWithExpirationAnnotation(id, NEVER_EXPIRE);

        template.insert(document);

        DocumentWithExpirationAnnotation byId = template.findById(id, DocumentWithExpirationAnnotation.class);
        assertThat(byId).isNotNull();
        Record record = client.get(null, new Key(namespace, "expiration-set", id));
        assertThat(record.getTimeToLive()).isEqualTo(NEVER_EXPIRE);
    }

    @Test
    void shouldNotUpdateExpirationWhenRecordIsUpdatedForNotUpdateExpirationPolicy() {
        DocumentWithExpirationAnnotation document = new DocumentWithExpirationAnnotation(id, 10);

        template.insert(document);
        Record record = client.get(null, new Key(namespace, "expiration-set", document.getId()));

        template.update(new DocumentWithExpirationAnnotation(id, DO_NOT_UPDATE_EXPIRATION));

        Record recordAfterUpdate = client.get(null, new Key(namespace, "expiration-set", document.getId()));
        assertThat(record.getTimeToLive()).isEqualTo(recordAfterUpdate.getTimeToLive());
    }

    @Test
    void invalidExpirationValueThrowsException() {
        DocumentWithExpirationAnnotation document = new DocumentWithExpirationAnnotation(id, -500);

        assertThatThrownBy(() -> template.insert(document))
                .isInstanceOf(InvalidDataAccessApiUsageException.class)
                .hasCauseInstanceOf(AerospikeException.class)
                .hasStackTraceContaining("Parameter error");
    }
}
