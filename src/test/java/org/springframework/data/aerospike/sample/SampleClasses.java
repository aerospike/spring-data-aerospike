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
import com.aerospike.dsl.api.DSLParser;
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
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
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
import java.time.Instant;
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
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SimpleClass implements SomeInterface {

        public static final String SIMPLESET = "simpleset1";
        @Id
        long id;
        String field1;
        int field2;
        long field3;
        float field4;
        double field5;
        boolean field6;
        Date field7;
        TYPES field8;
        Set<String> field9;
        Set<Set<String>> field10;
        byte field11;
        char field12;
        char field13;
    }

    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MapWithShortKey {

        @Id
        long id;
        Map<Short, String> mapWithShortKey;
    }


    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MapWithIntegerKey {

        @Id
        long id;
        Map<Integer, String> mapWithIntKey;
    }

    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MapWithLongKey {

        @Id
        long id;
        Map<Long, String> mapWithLongKey;
    }

    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MapWithDoubleKey {

        @Id
        long id;
        Map<Double, String> mapWithDoubleKey;
    }

    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MapWithByteKey {

        @Id
        long id;
        Map<Byte, String> mapWithByteKey;
    }

    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MapWithCharacterKey {

        @Id
        long id;
        Map<Character, String> mapWithCharacterKey;
    }

    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MapWithStringValue {

        @Id
        long id;
        Map<String, String> mapWithStringValue;
    }

    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MapWithCollectionValue {

        @Id
        long id;
        Map<String, List<String>> mapWithCollectionValue;
    }

    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MapWithGenericValue<T> {

        @Id
        long id;
        Map<String, T> mapWithNonSimpleValue;
    }

    @Data
    @Document
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DocumentExample {

        @Id
        String id;
        Map<SomeId, SomeEntity> entityMap;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @FieldDefaults(level = AccessLevel.PRIVATE)
    @Document(collection = "${aerospike.collections.test}")
    public static class DocumentExampleIdClass {

        @Id
        DocumentExample id;
        long counter;
        @Version
        int version; // the value must be effectively integer
        @Field("update")
        Long timestamp;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SomeId {

        String partA;
        String partB;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SomeEntity {

        SomeId id;
        String fieldA;
        Long fieldB;
    }

    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BigDecimalContainer {

        @Id
        String id;
        BigDecimal value;
        Map<String, BigDecimal> map;
        List<BigDecimal> collection;
    }

    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CustomTypeWithListAndMap {

        @Id
        String id;
        List<Object> listOfObjects;
        Map<String, Object> mapWithObjectValue;
    }

    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CustomTypeWithCustomType {

        @Id
        String id;
        ImmutableListAndMap field;
    }

    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CustomTypeWithListAndMapImmutable {

        @Id
        String id;
        List<Object> listOfObjects;
        Map<String, Object> mapWithObjectValue;
    }

    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CustomTypeWithCustomTypeImmutable {

        ImmutableListAndMap field;
    }

    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SortedMapWithSimpleValue {

        @Id
        String id;
        SortedMap<String, String> map;
    }

    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class NestedMapsWithSimpleValue {

        @Id
        String id;
        Map<String, Map<String, Map<String, String>>> nestedMaps;
    }

    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class GenericType<T> {

        @Id
        String id;
        T content;
    }

    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CollectionOfObjects {

        @Id
        String id;
        Collection<Object> collection;
    }

    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ListOfLists {

        @Id
        String id;
        List<List<String>> listOfLists;
    }

    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ListOfMaps {

        @Id
        String id;
        List<Map<String, Name>> listOfMaps;
    }

    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ContainerOfCustomFieldNames {

        @Id
        String id;
        @Field("property")
        String myField;
        CustomFieldNames customFieldNames;
    }

    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CustomFieldNames {

        @Field("property1")
        int intField;
        @Field("property2")
        String stringField;
    }

    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ClassWithComplexId {

        @Id
        ComplexId complexId;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ComplexId {

        Long innerId;
    }

    @Document
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SetWithSimpleValue {

        @Id
        long id;
        Set<String> collectionWithSimpleValues;
    }

    @Document(collection = SIMPLESET2)
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SimpleClassWithPersistenceConstructor {

        public static final String SIMPLESET2 = "simpleset2";
        @Id
        long id;
        String field1;
        int field2;
    }

    @Document(collection = SIMPLESET3)
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class User implements Contact {

        public static final String SIMPLESET3 = "simpleset3";
        @Id
        long id;
        Name name;
        Address address;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ImmutableListAndMap {

        List<Object> listOfObjects;
        Map<String, Object> mapWithObjectValue;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Name {

        String firstName;
        String lastName;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Address {

        Street street;
        int apartment;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Street {

        String name;
        int number;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DocumentWithPrimitiveShortId {

        @Id
        private short id;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DocumentWithPrimitiveIntId {

        @Id
        private int id;
    }

    @Data
    @NoArgsConstructor
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
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DocumentWithShortId {

        @Id
        private Short id;
        private String content;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DocumentWithIntegerId {

        @Id
        private Integer id;
        private String content;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DocumentWithLongId {

        @Id
        private Long id;
        private String content;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DocumentWithCharacterId {

        @Id
        private Character id;
        private String content;
    }

    @Data
    @Builder
    @NoArgsConstructor
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
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    @EqualsAndHashCode
    public static class Person implements Contact {

        @Id
        String id;
        Set<Address> addresses;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class IdAndAddressesList {

        @Id
        String id;
        List<Address> addresses;
    }

    @Data
    @Document(collection = "versioned-set")
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VersionedClass {

        @Version
        private int version; // the value must be effectively integer
        private String field;
        @Id
        private String id;

        public VersionedClass(String id, String field, int version) {
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
    @NoArgsConstructor
    public static class VersionedClassWithAllArgsConstructor {

        private String field;
        @Id
        private String id;
        @Version
        private int version; // the value must be effectively integer

        @PersistenceCreator
        public VersionedClassWithAllArgsConstructor(String id, String field, int version) {
            this.id = id;
            this.field = field;
            this.version = version;
        }
    }

    @Getter
    @EqualsAndHashCode
    @ToString
    @Document(collection = "custom-set")
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CustomCollectionClass {

        @Id
        private String id;
        private String data;
    }

    @Data
    @Document(collection = "set-to-delete")
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CustomCollectionClassToDelete {

        @Id
        String id;
    }

    @Data
    @NoArgsConstructor
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
    @NoArgsConstructor
    @AllArgsConstructor
    @Document(collection = "expiration-set", expiration = 1, touchOnRead = true)
    public static class DocumentWithTouchOnRead {

        @Id
        private String id;

        private int field;

        @Version
        private int version; // the value must be effectively integer

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
    @NoArgsConstructor
    @AllArgsConstructor
    @Document(collection = "expiration-set")
    public static class DocumentWithExpirationAnnotation {

        @Id
        private String id;

        @Expiration
        private Integer expiration; // the value must be effectively integer
    }

    @Data
    @Document(collection = "expiration-set")
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DocumentWithExpirationAnnotationAndPersistenceConstructor {

        @Id
        String id;

        @Expiration
        int expiration; // the value must be effectively integer
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Document(collection = "expiration-set")
    public static class DocumentWithUnixTimeExpiration {

        @Id
        private String id;

        @Expiration(unixTime = true)
        private DateTime expiration;
    }

    @Document(expirationExpression = "${expirationProperty}")
    @NoArgsConstructor
    public static class DocumentWithExpirationExpression {

    }

    @Document(expiration = EXPIRATION_ONE_SECOND, expirationUnit = TimeUnit.MINUTES)
    @NoArgsConstructor
    public static class DocumentWithExpirationUnit {

    }

    @Document(expiration = EXPIRATION_NEVER_EXPIRE, expirationUnit = TimeUnit.DAYS)
    @NoArgsConstructor
    public static class DocumentWithNeverExpireAndExpirationUnit {

    }

    @Document(expiration = EXPIRATION_NEVER_EXPIRE)
    @NoArgsConstructor
    public static class DocumentWithNeverExpireAndWithoutExpirationUnit {

    }

    @Document
    @NoArgsConstructor
    public static class DocumentWithoutExpiration {

    }

    @NoArgsConstructor
    public static class DocumentWithoutAnnotation {

    }

    @Document(expiration = 1, expirationExpression = "${expirationProperty}")
    @NoArgsConstructor
    public static class DocumentWithExpirationAndExpression {

    }

    @Data
    @Document
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DocumentWithDefaultConstructor {

        @Id
        private String id;

        @Expiration(unixTime = true)
        private DateTime expiration;

        private int intField;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Document(collection = "expiration-set", expiration = 1, expirationUnit = TimeUnit.DAYS, touchOnRead = true)
    public static class DocumentWithExpirationOneDay {

        @Id
        private String id;
        @Version
        private long version; // the value must be effectively integer

        @PersistenceCreator
        public DocumentWithExpirationOneDay(String id) {
            this.id = id;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Document(collection = "expiration-set", touchOnRead = true)
    public static class DocumentWithTouchOnReadAndExpirationProperty {

        @Id
        private String id;
        @Expiration
        private int expiration; // the value must be effectively integer
    }

    @Document(collection = DocumentWithExpressionInCollection.COLLECTION_PREFIX + "${setSuffix}")
    @NoArgsConstructor
    public static class DocumentWithExpressionInCollection {

        public static final String COLLECTION_PREFIX = "set-prefix-";
    }

    @Document
    @NoArgsConstructor
    public static class DocumentWithoutCollection {

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Document
    public static class DocumentWithByteArray {

        @Id
        private String id;
        @Field
        private byte[] array;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Document
    public static class DocumentWithIntArray {

        @Id
        private String id;
        @Field
        private int[] array;
    }

    @Data
    @NoArgsConstructor
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
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ObjectWithIntegerArray {

        @Version
        private Integer version; // the value must be effectively integer
        Integer[] array;

        public ObjectWithIntegerArray(Integer[] array) {
            this.array = array;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Document
    public static class DocumentWithByteArrayList {

        @Id
        private String id;
        @Field
        private List<Byte> array;
    }

    @Data
    @NoArgsConstructor
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
    @NoArgsConstructor
    @AllArgsConstructor
    @Document
    public static class DocumentWithURL {

        @Id
        private String id;
        @Field
        private URL url;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Document
    public static class DocumentWithUUID {

        @Id
        private String id;
        @Field
        private UUID uuid;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Document
    public static class DocumentWithCurrency {

        @Id
        private String id;
        @Field
        private Currency currency;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Document
    public static class DocumentWithDate {

        @Id
        private String id;
        @Field
        private Date date;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Document
    public static class DocumentWithCalendar {

        @Id
        private String id;
        @Field
        private Calendar calendar;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Document
    public static class DocumentWithInstant {
        @Id
        private String id;
        @Field
        private Instant instant;
    }

    @Data
    @NoArgsConstructor
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
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DocumentWithCompositeKey {

        @Id
        private CompositeKey id;

        @Field
        private String data;

        public DocumentWithCompositeKey(CompositeKey id) {
            this.id = id;
            this.data = "some-initial-data";
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
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
