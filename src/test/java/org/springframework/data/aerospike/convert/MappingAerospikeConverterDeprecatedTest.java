/*
 * Copyright 2020 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.convert;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.ConversionNotSupportedException;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.aerospike.convert.MappingAerospikeConverterDeprecatedTest.ClassWithMapUsingEnumAsKey.FooBarEnum;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.aerospike.mapping.Field;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.convert.CustomConversions;

import java.math.BigInteger;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
//TODO: cleanup me and move needed tests to MappingAerospikeConverterTest
public class MappingAerospikeConverterDeprecatedTest {

	MappingAerospikeConverter converter;
	Key key;

	private static final String AEROSPIKE_KEY = "AerospikeKey";
	private static final String AEROSPIKE_SET_NAME = "AerospikeSetName";
	private static final String AEROSPIKE_NAME_SPACE = "AerospikeNameSpace";
	private CustomConversions customConversions = new AerospikeCustomConversions(Collections.emptyList());
	private AerospikeTypeAliasAccessor aerospikeTypeAliasAccessor = new AerospikeTypeAliasAccessor();

	@BeforeEach
	public void setUp() {
		converter = new MappingAerospikeConverter(new AerospikeMappingContext(), customConversions, aerospikeTypeAliasAccessor);
		converter.afterPropertiesSet();
		key = new Key(AEROSPIKE_NAME_SPACE, AEROSPIKE_SET_NAME, AEROSPIKE_KEY);
	}

	@Test
	public void mappingAerospikeConverter() {
		MappingAerospikeConverter mappingAerospikeConverter = new MappingAerospikeConverter(new AerospikeMappingContext(), customConversions, aerospikeTypeAliasAccessor);
		assertThat(mappingAerospikeConverter.getConversionService()).isNotNull();
	}

	@Test
	public void getConversionService() {
		MappingAerospikeConverter mappingAerospikeConverter = new MappingAerospikeConverter(new AerospikeMappingContext(), customConversions, aerospikeTypeAliasAccessor);
		assertThat(mappingAerospikeConverter.getConversionService()).isNotNull().isInstanceOf(DefaultConversionService.class);
	}

	@Test
	public void convertsAddressCorrectlyToAerospikeData() {
		Address address = new Address();
		address.city = "New York";
		address.street = "Broadway";

		AerospikeWriteData dbObject = AerospikeWriteData.forWrite(key.namespace);
		converter.write(address, dbObject);

		Collection<Bin> bins = dbObject.getBins();

		assertThat(bins).contains(new Bin("city", "New York"), new Bin("street", "Broadway"));
	}

	@SuppressWarnings("serial")
	@Test
	public void convertsAerospikeDataToAddressCorrectly() {
		Address address = new Address();
		address.city = "New York";
		address.street = "Broadway";

		Map<String, Object> bins = new HashMap<String, Object>() {
			{
				put("city", "New York");
				put("street", "Broadway");
			}
		};

		AerospikeReadData dbObject = AerospikeReadData.forRead(key, aeroRecord(bins));

		Address convertedAddress = converter.read(Address.class, dbObject);

		assertThat(convertedAddress).isEqualTo(address);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void writesMapTypeCorrectly() {
		ClassWithMapProperty foo = new ClassWithMapProperty();

		foo.map = Collections.singletonMap(Locale.US, "Biff");

		AerospikeWriteData dbObject = AerospikeWriteData.forWrite(key.namespace);
		converter.write(foo, dbObject);

		Object object = getBinValue("map", dbObject.getBins());
		assertThat((Map)object).containsEntry(Locale.US.toString(), "Biff");
	}

	@Test
	public void writesEnumsCorrectly() {
		ClassWithEnumProperty value = new ClassWithEnumProperty();
		value.sampleEnum = SampleEnum.FIRST;

		AerospikeWriteData result = AerospikeWriteData.forWrite(key.namespace);
		converter.write(value, result);

		Object object = getBinValue("sampleEnum", result.getBins());
		//all Enums are saved in form of String in the DB
		assertThat(object).isInstanceOf(String.class);
		assertThat(SampleEnum.valueOf(object.toString())).isEqualTo(SampleEnum.FIRST);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void writesEnumCollectionCorrectly() {
		ClassWithEnumProperty value = new ClassWithEnumProperty();
		value.enums = Arrays.asList(SampleEnum.FIRST);

		AerospikeWriteData result = AerospikeWriteData.forWrite(key.namespace);
		converter.write(value, result);

		Object object = getBinValue("enums", result.getBins());

		assertThat((List) object).hasSize(1);
		assertThat((List) object).containsOnly("FIRST");
	}

	@SuppressWarnings("serial")
	@Test
	public void readsEnumsCorrectly() {
		Map<String, Object> bins = new HashMap<String, Object>() {
			{
				put("sampleEnum", SampleEnum.FIRST);
			}
		};

		AerospikeReadData dbObject = AerospikeReadData.forRead(key, aeroRecord(bins));

		ClassWithEnumProperty result = converter.read(ClassWithEnumProperty.class, dbObject);

		assertThat(result.sampleEnum).isEqualTo(SampleEnum.FIRST);
	}

	@SuppressWarnings("serial")
	@Test
	public void readsEnumCollectionsCorrectly() {
		Map<String, Object> bins = new HashMap<String, Object>() {
			{
				put("sampleEnum", SampleEnum.FIRST);
				put("enums", Arrays.asList(SampleEnum.FIRST));
			}
		};

		AerospikeReadData dbObject = AerospikeReadData.forRead(key, aeroRecord(bins));

		ClassWithEnumProperty result = converter.read(ClassWithEnumProperty.class, dbObject);

		assertThat(result.enums).isInstanceOf(List.class);
		assertThat(result.enums).hasSize(1);
		assertThat(result.enums).contains(SampleEnum.FIRST);
	}

	@Test
	public void considersFieldNameAnnotationWhenWriting() {
		Person person = new Person();
		person.id = "oliver-01";
		person.firstname = "Oliver";

		AerospikeWriteData dbObject = AerospikeWriteData.forWrite(key.namespace);
		converter.write(person, dbObject);


		Collection<Bin> bins = dbObject.getBins();
		Object foo = getBinValue("foo", bins);
		Object firstName = getBinValue("firstname", bins);

		assertThat(foo).isEqualTo("Oliver");
		assertThat(firstName).isNull();
	}

	@SuppressWarnings("serial")
	@Test
	public void considersFieldNameAnnotationWhenReading() {
		Map<String, Object> bins = new HashMap<String, Object>() {
			{
				put("id", "id1");
				put("birthDate", null);
				put("foo", "Oliver");
			}
		};

		AerospikeReadData dbObject = AerospikeReadData.forRead(key, aeroRecord(bins));

		Person result = converter.read(Person.class, dbObject);

		assertThat(result.firstname).isEqualTo("Oliver");
	}

	@Test
	public void readsEmptySetsCorrectly() {
		Person person = new Person();
		person.id = "oliver-02";
		person.addresses = Collections.emptySet();

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite(key.namespace);

		converter.write(person, forWrite);

		Map<String, Object> bins = listToMap(forWrite.getBins());
		AerospikeReadData forRead = AerospikeReadData.forRead(key, aeroRecord(bins));
		Person result = converter.read(Person.class, forRead);

		assertThat(result.addresses).isEmpty();
	}

	@SuppressWarnings("serial")
	@Test
	public void convertsCustomEmptyMapCorrectly() {
		final Map<String, Object> map = new HashMap<String, Object>() {
			{
				put("city", "New York");
				put("street", "Broadway");
			}
		};

		Map<String, Object> bins = new HashMap<String, Object>() {
			{
				put("map", map);
			}
		};
		AerospikeReadData dbObject = AerospikeReadData.forRead(key, aeroRecord(bins));

		ClassWithSortedMap result = converter.read(ClassWithSortedMap.class, dbObject);

		assertThat(result).isInstanceOf(ClassWithSortedMap.class);
		assertThat(result.map).isInstanceOf(Map.class);
	}



	/**
	 * @param bins
	 * @return
	 */
	private Map<String, Object> listToMap(Collection<Bin> bins) {
		Map<String, Object> map = new HashMap<>();
		if (bins != null && bins.size() > 0) {
			for (Bin bin : bins) map.put(bin.name, bin.value.getObject());
		}
		return map;
	}

	private Object getBinValue(String name, Collection<Bin> bins) {
		if (bins == null || bins.isEmpty())
			return null;

		return bins.stream()
				.filter(bin -> bin.name.equals(name))
				.map(bin -> bin.value.getObject())
				.findFirst().orElse(null);
	}

	private Record aeroRecord(Map<String, Object> bins) {
		return new Record(bins, 0, 0);
	}

	static class GenericType<T> {
		T content;
	}

	static class ClassWithEnumProperty {

		SampleEnum sampleEnum;
		List<SampleEnum> enums;
		EnumSet<SampleEnum> enumSet;
		EnumMap<SampleEnum, String> enumMap;
	}

	enum SampleEnum {
		FIRST {
			@Override
			void method() {
			}
		},
		SECOND {
			@Override
			void method() {

			}
		};

		abstract void method();
	}

	interface InterfaceType {

	}

	static class Address implements InterfaceType {
		String street;
		String city;

		@Override
		public String toString() {
			return "Address [street=" + street + ", city=" + city + "]";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((city == null) ? 0 : city.hashCode());
			result = prime * result
					+ ((street == null) ? 0 : street.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Address other = (Address) obj;
			if (city == null) {
				if (other.city != null)
					return false;
			} else if (!city.equals(other.city))
				return false;
			if (street == null) {
				return other.street == null;
			} else return street.equals(other.street);
		}
	}

	interface Contact {

	}

	static class Person implements Contact {
		@Id
		String id;

		Date birthDate;

		@Field("foo")
		String firstname;
		String lastname;

		Set<Address> addresses;

		public Person() {

		}

		public Person(Set<Address> addresses) {
			this.addresses = addresses;
		}
	}

	static class ClassWithSortedMap {
		SortedMap<String, String> map;
	}

	static class ClassWithMapProperty {
		Map<Locale, String> map;
		Map<String, List<String>> mapOfLists;
		Map<String, Object> mapOfObjects;
		Map<String, String[]> mapOfStrings;
		Map<String, Person> mapOfPersons;
		TreeMap<String, Person> treeMapOfPersons;
	}

	static class ClassWithNestedMaps {
		Map<String, Map<String, Map<String, String>>> nestedMaps;
	}

	static class BirthDateContainer {
		Date birthDate;
	}

	static class CollectionWrapper {
		List<Contact> contacts;
		List<List<String>> strings;
		List<Map<String, Locale>> listOfMaps;
		Set<Contact> contactsSet;
	}

	static class LocaleWrapper {
		Locale locale;
	}

	static class ClassWithBigIntegerId {
		@Id
		BigInteger id;
	}

	static class A<T> {
		String valueType;
		T value;

		public A(T value) {
			this.valueType = value.getClass().getName();
			this.value = value;
		}
	}

	static class DefaultedConstructorArgument {
		String foo;
		int bar;
		double foobar;

		DefaultedConstructorArgument(String foo, int bar, double foobar) {
			this.foo = foo;
			this.bar = bar;
			this.foobar = foobar;
		}
	}

	static class Item {
		List<Attribute> attributes;
	}

	static class Attribute {
		String key;
		Object value;
	}

	static class Outer {
		static class Inner {
			String value;
		}

		Inner inner;
	}

	static class URLWrapper {
		URL url;
	}

	static class ClassWithComplexId {
		@Id
		ComplexId complexId;
	}

	static class ComplexId {
		Long innerId;
	}

	static class TypWithCollectionConstructor {
		List<Attribute> attributes;

		public TypWithCollectionConstructor(List<Attribute> attributes) {
			this.attributes = attributes;
		}
	}

	@TypeAlias("_")
	static class Aliased {
		String name;
	}

	static class ThrowableWrapper {
		Throwable throwable;
	}

	@Document
	static class PrimitiveContainer {
		@Field("property")
		private final int m_property;

		public PrimitiveContainer(int a_property) {
			m_property = a_property;
		}

		public int property() {
			return m_property;
		}
	}

	@Document
	static class ObjectContainer {
		@Field("property")
		private final PrimitiveContainer m_property;

		public ObjectContainer(PrimitiveContainer a_property) {
			m_property = a_property;
		}

		public PrimitiveContainer property() {
			return m_property;
		}
	}

	static class RootForClassWithExplicitlyRenamedIdField {
		@Id
		String id;
		ClassWithExplicitlyRenamedField nested;
	}

	static class ClassWithExplicitlyRenamedField {
		@Field("id")
		String id;
	}

	static class RootForClassWithNamedIdField {
		String id;
		ClassWithNamedIdField nested;
	}

	static class ClassWithNamedIdField {
		String id;
	}

	static class ClassWithAnnotatedIdField {
		@Id
		String key;
	}

	static class ClassWithMapUsingEnumAsKey {
		enum FooBarEnum {
			FOO, BAR
		}

		Map<FooBarEnum, String> map;
	}

	static class FooBarEnumToStringConverter implements Converter<FooBarEnum, String> {
		@Override
		public String convert(FooBarEnum source) {

			if (source == null) {
				return null;
			}

			return FooBarEnum.FOO.equals(source) ? "foo-enum-value" : "bar-enum-value";
		}
	}

	static class StringToFooNumConverter implements Converter<String, FooBarEnum> {
		@Override
		public FooBarEnum convert(String source) {

			if (source == null) {
				return null;
			}

			if (source.equals("foo-enum-value")) {
				return FooBarEnum.FOO;
			}
			if (source.equals("bar-enum-value")) {
				return FooBarEnum.BAR;
			}

			throw new ConversionNotSupportedException(source, String.class, null);
		}
	}

}
