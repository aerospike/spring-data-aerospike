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
package org.springframework.data.aerospike;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.Value;
import org.joda.time.DateTime;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.aerospike.annotation.Expiration;
import org.springframework.data.aerospike.convert.AerospikeReadData;
import org.springframework.data.aerospike.convert.AerospikeWriteData;
import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.aerospike.mapping.Field;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.annotation.Version;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import static org.springframework.data.aerospike.SampleClasses.SimpleClass.SIMPLESET;
import static org.springframework.data.aerospike.SampleClasses.SimpleClassWithPersistenceConstructor.SIMPLESET2;
import static org.springframework.data.aerospike.SampleClasses.User.SIMPLESET3;

public class SampleClasses {

	public static final int EXPIRATION_ONE_SECOND = 1;
	public static final int EXPIRATION_ONE_MINUTE = 60;

	static interface SomeInterface {
	}

	public static enum TYPES {
		FIRST(1), SECOND(2), THIRD(3);
		final int id;

		TYPES(int id) {
			this.id = id;
		}
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
//		final char field12;

	}

	@Document
	@Data
	public static class MapWithSimpleValue {
		@Id
		final long id;
		final Map<String, String> mapWithSimpleValue;
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
		final List<Object> listOfObjects;
		final Map<String, Object> mapWithObjectValue;
	}

	@Document
	@Data
	public static class CustomTypeWithCustomType {
		final CustomTypeWithListAndMap field;
	}

	@Document
	@Value
	public static class CustomTypeWithListAndMapImmutable {
		final List<Object> listOfObjects;
		final Map<String, Object> mapWithObjectValue;
	}

	@Document
	@Value
	public static class CustomTypeWithCustomTypeImmutable {
		final CustomTypeWithListAndMapImmutable field;
	}



	@Document
	@Data
	public static class SortedMapWithSimpleValue {
		final SortedMap<String, String> map;
	}

	@Document
	@Data
	public static class NestedMapsWithSimpleValue {
		final Map<String, Map<String, Map<String, String>>> nestedMaps;
	}

	@Document
	@Data
	public static class GenericType<T> {
		final T content;
	}

	@Document
	@Data
	public static class CollectionOfObjects {
		final Collection<Object> collection;
	}

	@Document
	@Data
	public static class ListOfLists {
		final List<List<String>> listOfLists;
	}

	@Document
	@Data
	public static class ListOfMaps {
		final List<Map<String, Name>> listOfMaps;
	}

	@Document
	@Data
	public static class ContainerOfCustomFieldNames {
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

		@PersistenceConstructor
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

	@EqualsAndHashCode
	public static class ClassWithIntId {
		@Id
		public int id;
	}

	public static interface Contact {

	}

	@Document(expiration = EXPIRATION_ONE_SECOND)
	@AllArgsConstructor
	@ToString
	@EqualsAndHashCode
	public static class Person implements Contact {
		@Id
		String id;
		Set<Address> addresses;

		public Person() {

		}

		public Person(Set<Address> addresses) {
			this.addresses = addresses;
		}
	}

	@Data
	@Document(collection = "versioned-set")
	public static class VersionedClass {

		@Id
		private String id;

		@Version
		public Long version;// do not change to primitive type. See #72 issue

		public String field;

		public VersionedClass(String id, String field, Long version) {
			this.id = id;
			this.field = field;
			this.version = version;
		}

		@PersistenceConstructor
		public VersionedClass(String id, String field) {
			this.id = id;
			this.field = field;
		}
	}

	@Getter
	public static class VersionedClassWithAllArgsConstructor {

		@Id
		private String id;

		@Version
		public long version;

		public String field;

		@PersistenceConstructor
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
		private String id;
		private String data;

		public CustomCollectionClass(String id, String data) {
			this.id = id;
			this.data = data;
		}
	}

	@Data
	@AllArgsConstructor
	@Document(expiration = EXPIRATION_ONE_SECOND)
	public static class DocumentWithExpiration {

		@Id
		private String id;
	}

	@AllArgsConstructor
	@Document
	@Data
	public static class EnumProperties {

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
			return new AerospikeWriteData(new Key("custom-namespace", "custom-set", Long.toString(user.id)), bins, 0);
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

		@PersistenceConstructor
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
		private final String id;

		@Expiration
		private final Long expiration;

		@PersistenceConstructor
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

	@AllArgsConstructor
	@Data
	public static class ClassWithIdField {
		private long id;
		private String field;
	}

	@Data
	@AllArgsConstructor
	@Document(collection = "expiration-set", expiration = 1, expirationUnit = TimeUnit.DAYS, touchOnRead = true)
	public static class DocumentWithExpirationOneDay {

		@Id
		private String id;
		@Version
		private long version;

		@PersistenceConstructor
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

		@PersistenceConstructor
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
