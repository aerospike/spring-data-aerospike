/*
 * Copyright 2012-2020 the original author or authors
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
package org.springframework.data.aerospike.convert;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import lombok.Data;
import org.assertj.core.data.Offset;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.aerospike.sample.SampleClasses;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.assertions.KeyAssert.assertThat;
import static org.springframework.data.aerospike.sample.SampleClasses.*;
import static org.springframework.data.aerospike.sample.SampleClasses.SimpleClass.SIMPLESET;
import static org.springframework.data.aerospike.sample.SampleClasses.User.SIMPLESET3;
import static org.springframework.data.aerospike.utility.AerospikeExpirationPolicy.DO_NOT_UPDATE_EXPIRATION;
import static org.springframework.data.aerospike.utility.AerospikeExpirationPolicy.NEVER_EXPIRE;
import static org.springframework.data.aerospike.utility.AsCollections.list;
import static org.springframework.data.aerospike.utility.AsCollections.of;
import static org.springframework.data.aerospike.utility.AsCollections.set;

public class MappingAerospikeConverterTests extends BaseMappingAerospikeConverterTest {

    @SuppressWarnings("SameParameterValue")
    private static int toRecordExpiration(int expiration) {
        ZonedDateTime documentExpiration = ZonedDateTime.now(ZoneOffset.UTC).plus(expiration, ChronoUnit.SECONDS);
        ZonedDateTime aerospikeExpirationOffset = ZonedDateTime.of(2010, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        return (int) Duration.between(aerospikeExpirationOffset, documentExpiration).getSeconds();
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    public void readsCollectionOfObjectsToSetByDefault(int converterOption) {
        MappingAerospikeConverter aerospikeConverter = getAerospikeMappingConverterByOption(converterOption);
        CollectionOfObjects object = new CollectionOfObjects("my-id", list(new Person(null,
            set(new SampleClasses.Address(new SampleClasses.Street("Zarichna", 1), 202)))));

        AerospikeWriteData forWrite = AerospikeWriteData.forWrite(NAMESPACE);

        aerospikeConverter.write(object, forWrite);

        AerospikeReadData forRead = AerospikeReadData.forRead(forWrite.getKey(), aeroRecord(forWrite.getBins()));
        CollectionOfObjects actual = aerospikeConverter.read(CollectionOfObjects.class, forRead);

        assertThat(actual).isEqualTo(
            new CollectionOfObjects("my-id", set(new Person(null,
                set(new SampleClasses.Address(new SampleClasses.Street("Zarichna", 1), 202))))));
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    public void shouldReadCustomTypeWithCustomTypeImmutable(int converterOption) {
        MappingAerospikeConverter aerospikeConverter = getAerospikeMappingConverterByOption(converterOption);
        Map<String, Object> bins = of("field", of(
            "listOfObjects", ImmutableList.of("firstItem", of("keyInList", "valueInList")),
            "mapWithObjectValue", of("map", of("key", "value"))
        ));
        AerospikeReadData forRead = AerospikeReadData.forRead(new Key(NAMESPACE, SIMPLESET, 10L), aeroRecord(bins));

        CustomTypeWithCustomTypeImmutable actual = aerospikeConverter.read(CustomTypeWithCustomTypeImmutable.class,
            forRead);

        CustomTypeWithCustomTypeImmutable expected =
            new CustomTypeWithCustomTypeImmutable(new SampleClasses.ImmutableListAndMap(ImmutableList.of("firstItem",
                of("keyInList", "valueInList")),
                of("map", of("key", "value"))));
        assertThat(actual).isEqualTo(expected);
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    public void usesDocumentsStoredTypeIfSubtypeOfRequest(int converterOption) {
        MappingAerospikeConverter aerospikeConverter = getAerospikeMappingConverterByOption(converterOption);
        Map<String, Object> bins = of(
            "@_class", Person.class.getName(),
            "addresses", list()
        );
        AerospikeReadData dbObject = AerospikeReadData.forRead(new Key(NAMESPACE, "Person", "kate-01"),
            aeroRecord(bins));

        Contact result = aerospikeConverter.read(Contact.class, dbObject);
        assertThat(result).isInstanceOf(Person.class);
    }

    @Test
    public void shouldWriteAndReadUsingCustomConverter() {
        MappingAerospikeConverter converter =
            getMappingAerospikeConverter(settings, new UserToAerospikeWriteDataConverter(),
                new AerospikeReadDataToUserConverter());

        AerospikeWriteData forWrite = AerospikeWriteData.forWrite(NAMESPACE);
        User user = new User(678, new Name("Nastya", "Smirnova"), null);
        converter.write(user, forWrite);

        assertThat(forWrite.getBins()).containsOnly(
            new Bin("fs", "Nastya"), new Bin("ls", "Smirnova")
        );

        Map<String, Object> bins = of("fs", "Nastya", "ls", "Smirnova");
        User read = converter.read(User.class, AerospikeReadData.forRead(forWrite.getKey(), aeroRecord(bins)));

        assertThat(read).isEqualTo(user);
    }

    @Test
    public void shouldThrowExceptionIfIdAnnotationIsNotGiven() {
        @Data
        class TestName {

            final String firstName;
            final String lastName;
        }

        MappingAerospikeConverter converter =
            getMappingAerospikeConverter(settings, new UserToAerospikeWriteDataConverter(),
                new AerospikeReadDataToUserConverter());

        AerospikeWriteData forWrite = AerospikeWriteData.forWrite(NAMESPACE);
        TestName name = new TestName("Bob", "Dewey");

        assertThatThrownBy(() -> converter.write(name, forWrite))
            .isInstanceOf(AerospikeException.class);
    }

    @Test
    public void shouldWriteAndReadUsingCustomConverterOnNestedMapKeyObject() {
        MappingAerospikeConverter converter =
            getMappingAerospikeConverter(settings, new SampleClasses.SomeIdToStringConverter(),
                new SampleClasses.StringToSomeIdConverter());

        AerospikeWriteData forWrite = AerospikeWriteData.forWrite(NAMESPACE);

        SampleClasses.SomeId someId1 = new SampleClasses.SomeId("partA", "partB1");
        SampleClasses.SomeId someId2 = new SampleClasses.SomeId("partA", "partB2");
        SampleClasses.SomeEntity someEntity1 = new SampleClasses.SomeEntity(someId1, "fieldA", 42L);
        SampleClasses.SomeEntity someEntity2 = new SampleClasses.SomeEntity(someId2, "fieldA", 42L);
        Map<SampleClasses.SomeId, SampleClasses.SomeEntity> entityMap = new HashMap<>();
        entityMap.put(someId1, someEntity1);
        entityMap.put(someId2, someEntity2);

        SampleClasses.DocumentExample documentExample = new SampleClasses.DocumentExample("someKey1", entityMap);
        converter.write(documentExample, forWrite);

        Map<String, Object> entityMapExpectedAfterConversion = new HashMap<>();

        for (Map.Entry<SampleClasses.SomeId, SampleClasses.SomeEntity> entry : entityMap.entrySet()) {
            String newSomeIdAsStringKey = entry.getKey().getPartA() + "-" + entry.getKey().getPartB();
            HashMap<String, Object> newEntityMap = new HashMap<>();
            newEntityMap.put("id", entry.getValue().getId().getPartA() + "-" + entry.getValue().getId().getPartB());
            newEntityMap.put("fieldA", entry.getValue().getFieldA());
            newEntityMap.put("fieldB", entry.getValue().getFieldB());
            newEntityMap.put("@_class", "org.springframework.data.aerospike.sample.SampleClasses$SomeEntity");
            entityMapExpectedAfterConversion.put(newSomeIdAsStringKey, newEntityMap);
        }

        assertThat(forWrite.getKey()).consistsOf(settings, "namespace",
            "DocumentExample", "someKey1");
        assertThat(forWrite.getBins().stream()
            .filter(x -> !x.name.equals("@_class"))).containsOnly(new Bin("entityMap",
            entityMapExpectedAfterConversion));

        Map<String, Object> bins = of("entityMap", entityMapExpectedAfterConversion);
        SampleClasses.DocumentExample read = converter.read(SampleClasses.DocumentExample.class,
            AerospikeReadData.forRead(forWrite.getKey(), aeroRecord(bins)));

        assertThat(read).isEqualTo(documentExample);
    }

    @Test
    public void shouldWriteAndReadIfTypeKeyIsNull() {
        MappingAerospikeConverter converter =
            getMappingAerospikeConverter(settings, new AerospikeTypeAliasAccessor(null));

        AerospikeWriteData forWrite = AerospikeWriteData.forWrite(NAMESPACE);
        User user = new User(678L, null, null);
        converter.write(user, forWrite);

        assertThat(forWrite.getKey()).consistsOf(settings, NAMESPACE, SIMPLESET3, user.getId());
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    public void shouldWriteExpirationValue(int converterOption) {
        MappingAerospikeConverter aerospikeConverter = getAerospikeMappingConverterByOption(converterOption);
        Person person = new Person("personId", Collections.emptySet());
        AerospikeWriteData forWrite = AerospikeWriteData.forWrite(NAMESPACE);

        aerospikeConverter.write(person, forWrite);

        assertThat(forWrite.getExpiration()).isEqualTo(EXPIRATION_ONE_SECOND);
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    public void shouldReadExpirationFieldValue(int converterOption) {
        MappingAerospikeConverter aerospikeConverter = getAerospikeMappingConverterByOption(converterOption);
        Key key = new Key(NAMESPACE, "docId", 10L);

        int recordExpiration = toRecordExpiration(EXPIRATION_ONE_MINUTE);
        Record record = new Record(Collections.emptyMap(), 0, recordExpiration);

        AerospikeReadData readData = AerospikeReadData.forRead(key, record);

        DocumentWithExpirationAnnotation forRead = aerospikeConverter.read(DocumentWithExpirationAnnotation.class,
            readData);
        // Because of converting record expiration to TTL in Record.getTimeToLive method,
        // we may have expected expiration minus one second
        assertThat(forRead.getExpiration()).isIn(EXPIRATION_ONE_MINUTE, EXPIRATION_ONE_MINUTE - 1);
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    public void shouldReadUnixTimeExpirationFieldValue(int converterOption) {
        MappingAerospikeConverter aerospikeConverter = getAerospikeMappingConverterByOption(converterOption);
        Key key = new Key(NAMESPACE, "docId", 10L);
        int recordExpiration = toRecordExpiration(EXPIRATION_ONE_MINUTE);
        Record record = new Record(Collections.emptyMap(), 0, recordExpiration);

        AerospikeReadData readData = AerospikeReadData.forRead(key, record);
        DocumentWithUnixTimeExpiration forRead = aerospikeConverter.read(DocumentWithUnixTimeExpiration.class,
            readData);

        DateTime actual = forRead.getExpiration();
        DateTime expected = DateTime.now().plusSeconds(EXPIRATION_ONE_MINUTE);
        assertThat(actual.getMillis()).isCloseTo(expected.getMillis(), Offset.offset(100L));
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    public void shouldWriteUnixTimeExpirationFieldValue(int converterOption) {
        MappingAerospikeConverter aerospikeConverter = getAerospikeMappingConverterByOption(converterOption);
        DateTime unixTimeExpiration = DateTime.now().plusSeconds(EXPIRATION_ONE_MINUTE);
        DocumentWithUnixTimeExpiration document = new DocumentWithUnixTimeExpiration("docId", unixTimeExpiration);

        AerospikeWriteData forWrite = AerospikeWriteData.forWrite(NAMESPACE);
        aerospikeConverter.write(document, forWrite);

        assertThat(forWrite.getExpiration()).isIn(EXPIRATION_ONE_MINUTE, EXPIRATION_ONE_MINUTE - 1);
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    public void shouldFailWithExpirationFromThePast(int converterOption) {
        MappingAerospikeConverter aerospikeConverter = getAerospikeMappingConverterByOption(converterOption);
        DateTime expirationFromThePast = DateTime.now().minusSeconds(EXPIRATION_ONE_MINUTE);
        DocumentWithUnixTimeExpiration document = new DocumentWithUnixTimeExpiration("docId", expirationFromThePast);

        AerospikeWriteData forWrite = AerospikeWriteData.forWrite(NAMESPACE);

        assertThatThrownBy(() -> aerospikeConverter.write(document, forWrite))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageStartingWith("Expiration value must be greater than zero, but was: ");
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    public void shouldWriteExpirationFieldValue(int converterOption) {
        MappingAerospikeConverter aerospikeConverter = getAerospikeMappingConverterByOption(converterOption);
        DocumentWithExpirationAnnotation document = new DocumentWithExpirationAnnotation("docId",
            EXPIRATION_ONE_SECOND);
        AerospikeWriteData forWrite = AerospikeWriteData.forWrite(NAMESPACE);

        aerospikeConverter.write(document, forWrite);

        assertThat(forWrite.getExpiration()).isEqualTo(EXPIRATION_ONE_SECOND);
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    public void shouldNotSaveExpirationFieldAsBin(int converterOption) {
        MappingAerospikeConverter aerospikeConverter = getAerospikeMappingConverterByOption(converterOption);
        DocumentWithExpirationAnnotation document = new DocumentWithExpirationAnnotation("docId",
            EXPIRATION_ONE_SECOND);
        AerospikeWriteData forWrite = AerospikeWriteData.forWrite(NAMESPACE);

        aerospikeConverter.write(document, forWrite);

        assertThat(forWrite.getBins()).doesNotContain(new Bin("expiration", Value.get(EXPIRATION_ONE_SECOND)));
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    public void shouldFailWithNullExpirationFieldValue(int converterOption) {
        MappingAerospikeConverter aerospikeConverter = getAerospikeMappingConverterByOption(converterOption);
        DocumentWithExpirationAnnotation document = new DocumentWithExpirationAnnotation("docId", null);
        AerospikeWriteData forWrite = AerospikeWriteData.forWrite(NAMESPACE);

        assertThatThrownBy(() -> aerospikeConverter.write(document, forWrite))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Expiration must not be null!");
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    public void shouldNotFailWithNeverExpirePolicy(int converterOption) {
        MappingAerospikeConverter aerospikeConverter = getAerospikeMappingConverterByOption(converterOption);
        DocumentWithExpirationAnnotation document = new DocumentWithExpirationAnnotation("docId", NEVER_EXPIRE);
        AerospikeWriteData forWrite = AerospikeWriteData.forWrite(NAMESPACE);

        aerospikeConverter.write(document, forWrite);

        assertThat(forWrite.getExpiration()).isEqualTo(NEVER_EXPIRE);
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    public void shouldNotFailWithDoNotUpdateExpirePolicy(int converterOption) {
        MappingAerospikeConverter aerospikeConverter = getAerospikeMappingConverterByOption(converterOption);
        DocumentWithExpirationAnnotation document = new DocumentWithExpirationAnnotation("docId",
            DO_NOT_UPDATE_EXPIRATION);
        AerospikeWriteData forWrite = AerospikeWriteData.forWrite(NAMESPACE);

        aerospikeConverter.write(document, forWrite);

        assertThat(forWrite.getExpiration()).isEqualTo(DO_NOT_UPDATE_EXPIRATION);
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    public void shouldReadExpirationForDocumentWithDefaultConstructor(int converterOption) {
        MappingAerospikeConverter aerospikeConverter = getAerospikeMappingConverterByOption(converterOption);
        int recordExpiration = toRecordExpiration(EXPIRATION_ONE_MINUTE);
        Record record = new Record(Collections.emptyMap(), 0, recordExpiration);
        Key key = new Key(NAMESPACE, "DocumentWithDefaultConstructor", "docId");
        AerospikeReadData forRead = AerospikeReadData.forRead(key, record);

        DocumentWithDefaultConstructor document = aerospikeConverter.read(DocumentWithDefaultConstructor.class,
            forRead);
        DateTime actual = document.getExpiration();
        DateTime expected = DateTime.now().plusSeconds(EXPIRATION_ONE_MINUTE);
        assertThat(actual.getMillis()).isCloseTo(expected.getMillis(), Offset.offset(100L));
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    public void shouldReadExpirationForDocumentWithPersistenceConstructor(int converterOption) {
        MappingAerospikeConverter aerospikeConverter = getAerospikeMappingConverterByOption(converterOption);
        int recordExpiration = toRecordExpiration(EXPIRATION_ONE_MINUTE);
        Record record = new Record(Collections.emptyMap(), 0, recordExpiration);
        Key key = new Key(NAMESPACE, "DocumentWithExpirationAnnotationAndPersistenceConstructor", "docId");
        AerospikeReadData forRead = AerospikeReadData.forRead(key, record);

        DocumentWithExpirationAnnotationAndPersistenceConstructor document =
            aerospikeConverter.read(DocumentWithExpirationAnnotationAndPersistenceConstructor.class, forRead);
        assertThat(document.getExpiration()).isCloseTo(TimeUnit.MINUTES.toSeconds(1), Offset.offset(100L));
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    public void shouldNotWriteVersionToBins(int converterOption) {
        MappingAerospikeConverter aerospikeConverter = getAerospikeMappingConverterByOption(converterOption);
        AerospikeWriteData forWrite = AerospikeWriteData.forWrite(NAMESPACE);
        aerospikeConverter.write(new VersionedClass("id", "data", 42L), forWrite);

        assertThat(forWrite.getBins()).containsOnly(
            new Bin("@_class", VersionedClass.class.getName()),
            new Bin("field", "data")
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    public void shouldReadObjectWithByteArrayFieldWithOneValueInData(int converterOption) {
        MappingAerospikeConverter aerospikeConverter = getAerospikeMappingConverterByOption(converterOption);
        Map<String, Object> bins = new HashMap<>();
        bins.put("array", 1);
        AerospikeReadData forRead = AerospikeReadData.forRead(new Key(NAMESPACE, "DocumentWithByteArray", "user-id"),
            aeroRecord(bins));

        DocumentWithByteArray actual = aerospikeConverter.read(DocumentWithByteArray.class, forRead);

        assertThat(actual).isEqualTo(new DocumentWithByteArray("user-id", new byte[]{1}));
    }

    @Test
    public void getConversionService() {
        MappingAerospikeConverter mappingAerospikeConverter =
            getMappingAerospikeConverter(settings, new AerospikeTypeAliasAccessor());
        assertThat(mappingAerospikeConverter.getConversionService()).isNotNull()
            .isInstanceOf(DefaultConversionService.class);
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    public void shouldConvertAddressCorrectlyToAerospikeData(int converterOption) {
        MappingAerospikeConverter aerospikeConverter = getAerospikeMappingConverterByOption(converterOption);
        Address address = new Address(new Street("Broadway", 30), 3);

        AerospikeWriteData dbObject = AerospikeWriteData.forWrite(NAMESPACE);
        dbObject.setKey(new Key(NAMESPACE, "Address", 90));
        aerospikeConverter.write(address, dbObject);

        Collection<Bin> bins = dbObject.getBins();
        assertThat(bins).contains(
            new Bin("@_class", "org.springframework.data.aerospike.sample.SampleClasses$Address"),
            new Bin("street",
                Map.of(
                    "@_class", "org.springframework.data.aerospike.sample.SampleClasses$Street",
                    "name", "Broadway",
                    "number", 30
                )
            ),
            new Bin("apartment", 3)
        );

        Object streetBin = getBinValue("street", dbObject.getBins());
        assertThat(streetBin).isInstanceOf(TreeMap.class);
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    public void shouldConvertAerospikeDataToAddressCorrectly(int converterOption) {
        MappingAerospikeConverter aerospikeConverter = getAerospikeMappingConverterByOption(converterOption);
        Address address = new Address(new Street("Broadway", 30), 3);

        Map<String, Object> bins = new TreeMap<>() {
            {
                put("street", Map.of(
                        "name", "Broadway",
                        "number", 30
                    )
                );
                put("apartment", 3);
            }
        };

        AerospikeReadData dbObject = AerospikeReadData.forRead(new Key(NAMESPACE, "Address", 90), aeroRecord(bins));
        Address convertedAddress = aerospikeConverter.read(Address.class, dbObject);

        assertThat(convertedAddress).isEqualTo(address);
    }
}
