/*
 * Copyright 2012-2018 the original author or authors
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
package org.springframework.data.aerospike.sample;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import lombok.*;
import lombok.experimental.FieldDefaults;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTime;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.aerospike.annotation.Expiration;
import org.springframework.data.aerospike.convert.AerospikeReadData;
import org.springframework.data.aerospike.convert.AerospikeWriteData;
import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.aerospike.mapping.Field;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.annotation.Version;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.util.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.springframework.data.aerospike.sample.SampleClasses.SimpleClass.SIMPLESET;
import static org.springframework.data.aerospike.sample.SampleClasses.SimpleClassWithPersistenceConstructor.SIMPLESET2;
import static org.springframework.data.aerospike.sample.SampleClasses.User.SIMPLESET3;

public class SampleClasses {

    public static final int EXPIRATION_NEVER_EXPIRE = -1;
    public static final int EXPIRATION_ONE_SECOND = 1;
    public static final int EXPIRATION_ONE_MINUTE = 60;

    public enum TYPES {
        FIRST(1), SECOND(2), THIRD(3);
        final int id;

        TYPES(int id) {
            this.id = id;
        }
    }

    interface SomeInterface {

    }

    public interface Contact {

    }

    @TypeAlias("simpleclass")
    @Document(collection = SIMPLESET)
    @Data
    public static class SimpleClass implements SomeInterface {

        public static final String SIMPLESET = "simpleset1";
        @Id
        final long id;
        final String field1;
        final int field2;
        final long field3;
        final float field4;
        final double field5;
        final boolean field6;
        final Date field7;
        final TYPES field8;
        final Set<String> field9;
        final Set<Set<String>> field10;
        final byte field11;
        final char field12;
        final char field13;
    }

    @Document
    @Data
    public static class MapWithShortId {

        @Id
        final long id;
        final Map<Short, String> mapWithShortId;
    }


    @Document
    @Data
    public static class MapWithIntegerId {

        @Id
        final long id;
        final Map<Integer, String> mapWithIntId;
    }

    @Document
    @Data
    public static class MapWithLongId {

        @Id
        final long id;
        final Map<Long, String> mapWithLongId;
    }

    @Document
    @Data
    public static class MapWithDoubleId {

        @Id
        final long id;
        final Map<Double, String> mapWithDoubleId;
    }

    @Document
    @Data
    public static class MapWithByteId {

        @Id
        final long id;
        final Map<Byte, String> mapWithByteId;
    }

    @Document
    @Data
    public static class MapWithCharacterId {

        @Id
        final long id;
        final Map<Character, String> mapWithCharacterId;
    }

    @Document
    @Data
    public static class MapWithStringValue {

        @Id
        final long id;
        final Map<String, String> mapWithStringValue;
    }

    @Document
    @Data
    public static class MapWithCollectionValue {

        @Id
        final long id;
        final Map<String, List<String>> mapWithCollectionValue;
    }

    @Document
    @Data
    public static class MapWithGenericValue<T> {

        @Id
        final long id;
        final Map<String, T> mapWithNonSimpleValue;
    }

    @Data
    @Document
    @AllArgsConstructor
    public static class DocumentExample {

        @Id
        String id;
        Map<SomeId, SomeEntity> entityMap;
    }

    @Data
    @AllArgsConstructor
    @FieldDefaults(level = AccessLevel.PRIVATE)
    @Document(collection = "${aerospike.collections.test}")
    public static class DocumentExampleIdClass {

        @Id
        DocumentExample id;
        long counter;
        @Version
        long version;
        @Field("update")
        Long timestamp;
    }

    @Value
    public static class SomeId {

        String partA;
        String partB;
    }

    @Value
    @AllArgsConstructor
    public static class SomeEntity {

        SomeId id;
        String fieldA;
        Long fieldB;
    }

    @Document
    @Value
    public static class BigDecimalContainer {

        @Id
        String id;
        BigDecimal value;
        Map<String, BigDecimal> map;
        List<BigDecimal> collection;
    }

    @Document
    @Data
    public static class CustomTypeWithListAndMap {

        @Id
        final String id;
        final List<Object> listOfObjects;
        final Map<String, Object> mapWithObjectValue;
    }

    @Document
    @Data
    public static class CustomTypeWithCustomType {

        @Id
        final String id;
        final ImmutableListAndMap field;
    }

    @Document
    @Value
    public static class CustomTypeWithListAndMapImmutable {

        @Id
        String id;
        List<Object> listOfObjects;
        Map<String, Object> mapWithObjectValue;
    }

    @Document
    @Value
    public static class CustomTypeWithCustomTypeImmutable {

        ImmutableListAndMap field;
    }

    @Document
    @Data
    public static class SortedMapWithSimpleValue {

        @Id
        final String id;
        final SortedMap<String, String> map;
    }

    @Document
    @Data
    public static class NestedMapsWithSimpleValue {

        @Id
        final String id;
        final Map<String, Map<String, Map<String, String>>> nestedMaps;
    }

    @Document
    @Data
    public static class GenericType<T> {

        @Id
        final String id;
        final T content;
    }

    @Document
    @Data
    public static class CollectionOfObjects {

        @Id
        final String id;
        final Collection<Object> collection;
    }

    @Document
    @Data
    public static class ListOfLists {

        @Id
        final String id;
        final List<List<String>> listOfLists;
    }

    @Document
    @Data
    public static class ListOfMaps {

        @Id
        final String id;
        final List<Map<String, Name>> listOfMaps;
    }

    @Document
    @Data
    public static class ContainerOfCustomFieldNames {

        @Id
        final String id;
        @Field("property")
        final String myField;
        final CustomFieldNames customFieldNames;
    }

    @Document
    @Data
    public static class CustomFieldNames {

        @Field("property1")
        final int intField;
        @Field("property2")
        final String stringField;
    }

    @Document
    @Data
    public static class ClassWithComplexId {

        @Id
        final ComplexId complexId;
    }

    @Data
    public static class ComplexId {

        final Long innerId;
    }

    @Document
    @Data
    public static class SetWithSimpleValue {

        @Id
        final long id;
        final Set<String> collectionWithSimpleValues;
    }

    @Document(collection = SIMPLESET2)
    @Data
    public static class SimpleClassWithPersistenceConstructor {

        public static final String SIMPLESET2 = "simpleset2";
        @Id
        final long id;
        final String field1;
        final int field2;

        @PersistenceCreator
        public SimpleClassWithPersistenceConstructor(long id, String field1, int field2) {
            this.id = id;
            this.field1 = field1;
            this.field2 = field2;
        }
    }

    @Document(collection = SIMPLESET3)
    @Data
    public static class User implements Contact {

        public static final String SIMPLESET3 = "simpleset3";
        @Id
        final long id;
        final Name name;
        final Address address;
    }

    @Value
    public static class ImmutableListAndMap {

        List<Object> listOfObjects;
        Map<String, Object> mapWithObjectValue;
    }

    @Data
    public static class Name {

        final String firstName;
        final String lastName;
    }

    @Data
    public static class Address {

        final Street street;
        final int apartment;
    }

    @Data
    public static class Street {

        final String name;
        final int number;
    }

    @Data
    @AllArgsConstructor
    public static class DocumentWithPrimitiveShortId {

        @Id
        private short id;
    }

    @Data
    @AllArgsConstructor
    public static class DocumentWithPrimitiveIntId {

        @Id
        private int id;
    }

    @Data
    @AllArgsConstructor
    public static class DocumentWithPrimitiveLongId {

        @Id
        private long id;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DocumentWithPrimitiveCharId {

        @Id
        private char id;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DocumentWithPrimitiveByteId {

        @Id
        private byte id;
    }

    @Data
    @Builder
    @AllArgsConstructor
    public static class DocumentWithShortId {

        @Id
        private Short id;
        private String content;
    }

    @Data
    @Builder
    @AllArgsConstructor
    public static class DocumentWithIntegerId {

        @Id
        private Integer id;
        private String content;
    }

    @Data
    @Builder
    @AllArgsConstructor
    public static class DocumentWithLongId {

        @Id
        private Long id;
        private String content;
    }

    @Data
    @Builder
    @AllArgsConstructor
    public static class DocumentWithCharacterId {

        @Id
        private Character id;
        private String content;
    }

    @Data
    @Builder
    @AllArgsConstructor
    public static class DocumentWithByteId {

        @Id
        private Byte id;
        private String content;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DocumentWithStringId {

        @Id
        private String id;
        private String content;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DocumentWithByteArrayId {

        @Id
        private byte[] id;
        private String content;
    }

    @Document(expiration = EXPIRATION_ONE_SECOND)
    @AllArgsConstructor
    @ToString
    @EqualsAndHashCode
    public static class Person implements Contact {

        @Id
        String id;
        Set<Address> addresses;

        private Person() {
        }

        private Person(Set<Address> addresses) {
            this.addresses = addresses;
        }
    }

    @Data
    @Document(collection = "versioned-set")
    public static class VersionedClass {

        @Version
        private long version;
        private String field;
        @Id
        private String id;

        public VersionedClass(String id, String field, long version) {
            this.id = id;
            this.field = field;
            this.version = version;
        }

        @PersistenceCreator
        public VersionedClass(String id, String field) {
            this.id = id;
            this.field = field;
        }
    }

    @Data
    @Document
    public static class VersionedClassWithAllArgsConstructor {

        private String field;
        @Id
        private String id;
        @Version
        private long version;

        @PersistenceCreator
        public VersionedClassWithAllArgsConstructor(String id, String field, long version) {
            this.id = id;
            this.field = field;
            this.version = version;
        }
    }

    @Getter
    @EqualsAndHashCode
    @ToString
    @Document(collection = "custom-set")
    public static class CustomCollectionClass {

        @Id
        private final String id;
        private final String data;

        public CustomCollectionClass(String id, String data) {
            this.id = id;
            this.data = data;
        }
    }

    @Value
    @Document(collection = "set-to-delete")
    public static class CustomCollectionClassToDelete {

        @Id
        String id;
    }

    @Data
    @AllArgsConstructor
    @Document(expiration = EXPIRATION_ONE_SECOND)
    public static class DocumentWithExpiration {

        @Id
        private String id;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Document
    @Data
    public static class ClassWithEnumProperties {

        @Id
        String id;
        TYPES type;
        List<TYPES> list;
        EnumSet<TYPES> set;
        EnumMap<TYPES, String> map;
    }

    @WritingConverter
    public static class ComplexIdToStringConverter implements Converter<ComplexId, String> {

        @Override
        public String convert(ComplexId complexId) {
            return "id::" + complexId.getInnerId();
        }
    }

    @ReadingConverter
    public static class StringToComplexIdConverter implements Converter<String, ComplexId> {

        @Override
        public ComplexId convert(String s) {
            long id = Long.parseLong(s.split("::")[1]);
            return new ComplexId(id);
        }
    }

    @WritingConverter
    public static class UserToAerospikeWriteDataConverter implements Converter<User, AerospikeWriteData> {

        @Override
        public AerospikeWriteData convert(User user) {
            Collection<Bin> bins = new ArrayList<>();
            bins.add(new Bin("fs", user.name.firstName));
            bins.add(new Bin("ls", user.name.lastName));
            return new AerospikeWriteData(new Key("custom-namespace", "custom-set", Long.toString(user.id)), bins, 0,
                null);
        }
    }

    @ReadingConverter
    public static class AerospikeReadDataToUserConverter implements Converter<AerospikeReadData, User> {

        @Override
        public User convert(AerospikeReadData source) {
            long id = Long.parseLong((String) source.getKey().userKey.getObject());
            String fs = (String) source.getValue("fs");
            String ls = (String) source.getValue("ls");
            return new User(id, new Name(fs, ls), null);
        }
    }

    @WritingConverter
    public static class SomeIdToStringConverter implements Converter<SomeId, String> {

        @Override
        public String convert(SomeId id) {
            return id.getPartA() + "-" + id.getPartB();
        }
    }

    @ReadingConverter
    public static class StringToSomeIdConverter implements Converter<String, SomeId> {

        @Override
        public SomeId convert(@NotNull String id) {
            String[] parts = StringUtils.split(id, "-");
            assert parts != null;
            return new SomeId(parts[0], parts[1]);
        }
    }

    @Data
    @AllArgsConstructor
    @Document(collection = "expiration-set", expiration = 1, touchOnRead = true)
    public static class DocumentWithTouchOnRead {

        @Id
        private String id;

        private int field;

        @Version
        private long version;

        public DocumentWithTouchOnRead(String id) {
            this(id, 0);
        }

        @PersistenceCreator
        public DocumentWithTouchOnRead(String id, int field) {
            this.id = id;
            this.field = field;
        }
    }

    @Data
    @AllArgsConstructor
    @Document(collection = "expiration-set")
    public static class DocumentWithExpirationAnnotation {

        @Id
        private String id;

        @Expiration
        private Integer expiration;
    }

    @Value
    @Document(collection = "expiration-set")
    public static class DocumentWithExpirationAnnotationAndPersistenceConstructor {

        @Id
        String id;

        @Expiration
        Long expiration;

        @PersistenceCreator
        public DocumentWithExpirationAnnotationAndPersistenceConstructor(String id, Long expiration) {
            this.id = id;
            this.expiration = expiration;
        }
    }

    @Data
    @AllArgsConstructor
    @Document(collection = "expiration-set")
    public static class DocumentWithUnixTimeExpiration {

        @Id
        private String id;

        @Expiration(unixTime = true)
        private DateTime expiration;
    }

    @Document(expirationExpression = "${expirationProperty}")
    public static class DocumentWithExpirationExpression {

    }

    @Document(expiration = EXPIRATION_ONE_SECOND, expirationUnit = TimeUnit.MINUTES)
    public static class DocumentWithExpirationUnit {

    }

    @Document(expiration = EXPIRATION_NEVER_EXPIRE, expirationUnit = TimeUnit.DAYS)
    public static class DocumentWithNeverExpireAndExpirationUnit {

    }

    @Document(expiration = EXPIRATION_NEVER_EXPIRE)
    public static class DocumentWithNeverExpireAndWithoutExpirationUnit {

    }

    @Document
    public static class DocumentWithoutExpiration {

    }

    public static class DocumentWithoutAnnotation {

    }

    @Document(expiration = 1, expirationExpression = "${expirationProperty}")
    public static class DocumentWithExpirationAndExpression {

    }

    @Data
    @Document
    public static class DocumentWithDefaultConstructor {

        @Id
        private String id;

        @Expiration(unixTime = true)
        private DateTime expiration;

        private int intField;
    }

    @Data
    @AllArgsConstructor
    @Document(collection = "expiration-set", expiration = 1, expirationUnit = TimeUnit.DAYS, touchOnRead = true)
    public static class DocumentWithExpirationOneDay {

        @Id
        private String id;
        @Version
        private long version;

        @PersistenceCreator
        public DocumentWithExpirationOneDay(String id) {
            this.id = id;
        }
    }

    @Data
    @AllArgsConstructor
    @Document(collection = "expiration-set", touchOnRead = true)
    public static class DocumentWithTouchOnReadAndExpirationProperty {

        @Id
        private String id;
        @Expiration
        private long expiration;
    }

    @Document(collection = DocumentWithExpressionInCollection.COLLECTION_PREFIX + "${setSuffix}")
    public static class DocumentWithExpressionInCollection {

        public static final String COLLECTION_PREFIX = "set-prefix-";
    }

    @Document
    public static class DocumentWithoutCollection {

    }

    @Data
    @AllArgsConstructor
    @Document
    public static class DocumentWithByteArray {

        @Id
        private String id;
        @Field
        private byte[] array;
    }

    @Data
    @AllArgsConstructor
    @Document
    public static class DocumentWithIntArray {

        @Id
        private String id;
        @Field
        private int[] array;
    }

    @Data
    @AllArgsConstructor
    @Document
    public static class DocumentWithBigIntegerAndNestedArray {

        @Id
        @NonNull
        private String id;
        @Field
        @NonNull
        private BigInteger bigInteger;
        private ObjectWithIntegerArray objectWithArray;
    }

    @Data
    public static class ObjectWithIntegerArray {

        @Version
        private Long version;
        Integer[] array;

        public ObjectWithIntegerArray(Integer[] array) {
            this.array = array;
        }
    }

    @Data
    @AllArgsConstructor
    @Document
    public static class DocumentWithByteArrayList {

        @Id
        private String id;
        @Field
        private List<Byte> array;
    }

    @Data
    @AllArgsConstructor
    @Document
    public static class DocumentWithAtomicFields {

        @Id
        private String id;
        @Field
        private AtomicInteger atomicInteger;
        @Field
        private AtomicLong atomicLong;
    }

    @Data
    @AllArgsConstructor
    @Document
    public static class DocumentWithURL {

        @Id
        private String id;
        @Field
        private URL url;
    }

    @Data
    @AllArgsConstructor
    @Document
    public static class DocumentWithUUID {

        @Id
        private String id;
        @Field
        private UUID uuid;
    }

    @Data
    @AllArgsConstructor
    @Document
    public static class DocumentWithCurrency {

        @Id
        private String id;
        @Field
        private Currency currency;
    }

    @Data
    @AllArgsConstructor
    @Document
    public static class DocumentWithDate {

        @Id
        private String id;
        @Field
        private Date date;
    }

    @Data
    @AllArgsConstructor
    @Document
    public static class DocumentWithCalendar {

        @Id
        private String id;
        @Field
        private Calendar calendar;
    }

    @Data
    @AllArgsConstructor
    @Document
    public static class DocumentWithDuration {

        @Id
        private String id;
        @Field
        private Duration duration;
    }

    @Data
    @Document
    public static class DocumentWithCompositeKey {

        @Id
        private CompositeKey id;

        @Field
        private String data;

        public DocumentWithCompositeKey(CompositeKey id) {
            this.id = id;
            this.data = "some-initial-data";
        }

        @PersistenceCreator
        public DocumentWithCompositeKey(CompositeKey id, String data) {
            this.id = id;
            this.data = data;
        }
    }

    @Value
    public static class CompositeKey {

        String firstPart;
        long secondPart;

        @WritingConverter
        public enum CompositeKeyToStringConverter implements Converter<CompositeKey, String> {
            INSTANCE;

            @Override
            public String convert(CompositeKey source) {
                return source.firstPart + "::" + source.secondPart;
            }
        }

        @ReadingConverter
        public enum StringToCompositeKeyConverter implements Converter<String, CompositeKey> {
            INSTANCE;

            @Override
            public CompositeKey convert(String source) {
                String[] split = source.split("::");
                return new CompositeKey(split[0], Long.parseLong(split[1]));
            }
        }
    }
}
