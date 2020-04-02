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

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.joda.time.DateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.context.ApplicationContext;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.env.Environment;
import org.springframework.data.aerospike.SampleClasses.Address;
import org.springframework.data.aerospike.SampleClasses.AerospikeReadDataToUserConverter;
import org.springframework.data.aerospike.SampleClasses.BigDecimalContainer;
import org.springframework.data.aerospike.SampleClasses.ClassWithComplexId;
import org.springframework.data.aerospike.SampleClasses.ClassWithIdField;
import org.springframework.data.aerospike.SampleClasses.ClassWithIntId;
import org.springframework.data.aerospike.SampleClasses.CollectionOfObjects;
import org.springframework.data.aerospike.SampleClasses.ComplexId;
import org.springframework.data.aerospike.SampleClasses.ComplexIdToStringConverter;
import org.springframework.data.aerospike.SampleClasses.Contact;
import org.springframework.data.aerospike.SampleClasses.ContainerOfCustomFieldNames;
import org.springframework.data.aerospike.SampleClasses.CustomFieldNames;
import org.springframework.data.aerospike.SampleClasses.CustomTypeWithCustomType;
import org.springframework.data.aerospike.SampleClasses.CustomTypeWithCustomTypeImmutable;
import org.springframework.data.aerospike.SampleClasses.CustomTypeWithListAndMap;
import org.springframework.data.aerospike.SampleClasses.CustomTypeWithListAndMapImmutable;
import org.springframework.data.aerospike.SampleClasses.DocumentWithByteArray;
import org.springframework.data.aerospike.SampleClasses.DocumentWithDefaultConstructor;
import org.springframework.data.aerospike.SampleClasses.DocumentWithExpirationAnnotation;
import org.springframework.data.aerospike.SampleClasses.DocumentWithExpirationAnnotationAndPersistenceConstructor;
import org.springframework.data.aerospike.SampleClasses.DocumentWithUnixTimeExpiration;
import org.springframework.data.aerospike.SampleClasses.EnumProperties;
import org.springframework.data.aerospike.SampleClasses.GenericType;
import org.springframework.data.aerospike.SampleClasses.ListOfLists;
import org.springframework.data.aerospike.SampleClasses.ListOfMaps;
import org.springframework.data.aerospike.SampleClasses.MapWithCollectionValue;
import org.springframework.data.aerospike.SampleClasses.MapWithGenericValue;
import org.springframework.data.aerospike.SampleClasses.MapWithSimpleValue;
import org.springframework.data.aerospike.SampleClasses.Name;
import org.springframework.data.aerospike.SampleClasses.NestedMapsWithSimpleValue;
import org.springframework.data.aerospike.SampleClasses.Person;
import org.springframework.data.aerospike.SampleClasses.SetWithSimpleValue;
import org.springframework.data.aerospike.SampleClasses.SimpleClass;
import org.springframework.data.aerospike.SampleClasses.SimpleClassWithPersistenceConstructor;
import org.springframework.data.aerospike.SampleClasses.SortedMapWithSimpleValue;
import org.springframework.data.aerospike.SampleClasses.Street;
import org.springframework.data.aerospike.SampleClasses.StringToComplexIdConverter;
import org.springframework.data.aerospike.SampleClasses.TYPES;
import org.springframework.data.aerospike.SampleClasses.User;
import org.springframework.data.aerospike.SampleClasses.UserToAerospikeWriteDataConverter;
import org.springframework.data.aerospike.SampleClasses.VersionedClass;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.convert.CustomConversions;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.data.aerospike.AsCollections.list;
import static org.springframework.data.aerospike.AsCollections.of;
import static org.springframework.data.aerospike.AsCollections.set;
import static org.springframework.data.aerospike.SampleClasses.EXPIRATION_ONE_MINUTE;
import static org.springframework.data.aerospike.SampleClasses.EXPIRATION_ONE_SECOND;
import static org.springframework.data.aerospike.SampleClasses.SimpleClass.SIMPLESET;
import static org.springframework.data.aerospike.SampleClasses.SimpleClassWithPersistenceConstructor.SIMPLESET2;
import static org.springframework.data.aerospike.SampleClasses.User.SIMPLESET3;
import static org.springframework.data.aerospike.assertions.KeyAssert.assertThat;

public class MappingAerospikeConverterTest {

	private static final String NAMESPACE = "namespace";
	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	ApplicationContext applicationContext = getApplicationContext();

	private MappingAerospikeConverter converter = getMappingAerospikeConverter(new ComplexIdToStringConverter(), new StringToComplexIdConverter());

	private MappingAerospikeConverter getMappingAerospikeConverter(Converter<?, ?>... customConverters) {
		return getMappingAerospikeConverter(new AerospikeTypeAliasAccessor(), customConverters);
	}

	private MappingAerospikeConverter getMappingAerospikeConverter(AerospikeTypeAliasAccessor typeAliasAccessor, Converter<?, ?>... customConverters) {
		AerospikeMappingContext mappingContext = new AerospikeMappingContext();
		mappingContext.setApplicationContext(applicationContext);
		mappingContext.setDefaultNameSpace(NAMESPACE);
		CustomConversions customConversions = new AerospikeCustomConversions(asList(customConverters));

		MappingAerospikeConverter converter = new MappingAerospikeConverter(mappingContext, customConversions, typeAliasAccessor);
		converter.afterPropertiesSet();
		return converter;
	}

	private ApplicationContext getApplicationContext() {
		Environment environment = mock(Environment.class);
		when(environment.resolveRequiredPlaceholders(anyString()))
				.thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));

		ApplicationContext applicationContext = mock(ApplicationContext.class);
		when(applicationContext.getEnvironment()).thenReturn(environment);

		return applicationContext;
	}

	@Test
	public void shouldReadNullObjectIfAerospikeDataNull() throws Exception {
		SimpleClass actual = converter.read(SimpleClass.class, null);

		assertThat(actual).isEqualTo(null);
	}

	@Test
	public void shouldWriteSetWithSimpleValue() throws Exception {
		SetWithSimpleValue object = new SetWithSimpleValue(1L, set("a", "b", "c", null));
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();

		converter.write(object, forWrite);

		assertThat(forWrite.getBins()).containsOnly(
				new Bin("collectionWithSimpleValues", list(null, "a", "b", "c")),
				new Bin("@user_key", "1"),
				new Bin("@_class", SetWithSimpleValue.class.getName())
		);

	}

	@Test
	public void shouldReadCollectionWithSimpleValue() throws Exception {
		Map<String, Object> bins = of(
				"collectionWithSimpleValues", list("a", "b", "c", "d", null),
				"@user_key", "10"
		);
		AerospikeReadData forRead = AerospikeReadData.forRead(new Key(NAMESPACE, SIMPLESET, 10L), record(bins));

		SetWithSimpleValue actual = converter.read(SetWithSimpleValue.class, forRead);

		Set<String> map = set("a", "b", "c", "d", null);
		SetWithSimpleValue expected = new SetWithSimpleValue(10L, map);
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void shouldWriteMapWithSimpleValue() throws Exception {
		Map<String, String> map = of("key1", "value1", "key2", "value2", "key3", null);
		MapWithSimpleValue object = new MapWithSimpleValue(10L, map);
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();

		converter.write(object, forWrite);

		assertThat(forWrite.getKey()).consistsOf(NAMESPACE, MapWithSimpleValue.class.getSimpleName(), 10L);
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("mapWithSimpleValue", of("key1", "value1", "key2", "value2", "key3", null)),
				new Bin("@user_key", "10"),
				new Bin("@_class", MapWithSimpleValue.class.getName())
		);
	}

	@Test
	public void shouldReadMapWithSimpleValue() throws Exception {
		Map<String, Object> bins = of(
				"mapWithSimpleValue", of("key1", "value1", "key2", "value2"),
				"@user_key", "10"
		);
		AerospikeReadData forRead = AerospikeReadData.forRead(new Key(NAMESPACE, SIMPLESET, 10L), record(bins));

		MapWithSimpleValue actual = converter.read(MapWithSimpleValue.class, forRead);

		Map<String, String> map = of("key1", "value1", "key2", "value2");
		MapWithSimpleValue expected = new MapWithSimpleValue(10L, map);
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void shouldWriteMapWithCollectionValues() throws Exception {
		Map<String, List<String>> map = of("key1", list(), "key2", list("a", "b", "c"));
		MapWithCollectionValue object = new MapWithCollectionValue(10L, map);
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();

		converter.write(object, forWrite);

		assertThat(forWrite.getKey()).consistsOf(NAMESPACE, MapWithCollectionValue.class.getSimpleName(), 10L);
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("mapWithCollectionValue", of("key1", list(), "key2", list("a", "b", "c"))),
				new Bin("@user_key", "10"),
				new Bin("@_class", MapWithCollectionValue.class.getName())
		);
	}

	@Test
	public void shouldReadMapWithCollectionValues() throws Exception {
		Map<String, Object> bins = of(
				"mapWithCollectionValue", of("key1", list(), "key2", list("a", "b", "c")),
				"@user_key", "10"
		);
		AerospikeReadData forRead = AerospikeReadData.forRead(new Key(NAMESPACE, SIMPLESET, 10L), record(bins));

		MapWithCollectionValue actual = converter.read(MapWithCollectionValue.class, forRead);

		Map<String, List<String>> map = of("key1", list(), "key2", list("a", "b", "c"));
		MapWithCollectionValue expected = new MapWithCollectionValue(10L, map);
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void shouldWriteMapWithNonSimpleValue() throws Exception {
		Map<String, Address> map = of("key1", new Address(new Street("Gogolya str.", 15), 567),
				"key2", new Address(new Street("Shakespeare str.", 40), 765));
		MapWithGenericValue<Address> object = new MapWithGenericValue<>(10L, map);
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();

		converter.write(object, forWrite);

		assertThat(forWrite.getKey()).consistsOf(NAMESPACE, MapWithGenericValue.class.getSimpleName(), 10L);
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("mapWithNonSimpleValue", of(
						"key1", of("street", of("name", "Gogolya str.", "number", 15, "@_class", Street.class.getName()),
								"apartment", 567, "@_class", Address.class.getName()),
						"key2", of("street", of("name", "Shakespeare str.", "number", 40, "@_class", Street.class.getName()),
								"apartment", 765, "@_class", Address.class.getName()))),
				new Bin("@user_key", "10"),
				new Bin("@_class", MapWithGenericValue.class.getName())
		);
	}

	@Test
	public void shouldReadListsAndMapsWithObjectValue() throws Exception {
		Map<String, Object> bins = of(
				"listOfObjects", ImmutableList.of("firstItem",
						of("keyInList", "valueInList"),
						of("street", of("name", "Gogolya str.", "number", 15, "@_class", Street.class.getName()),
								"apartment", 567, "@_class", Address.class.getName())),
				"mapWithObjectValue", of("map", of("key", "value")),
				"@_class", CustomTypeWithListAndMap.class.getName(),
				"@user_key", "10"
		);
		AerospikeReadData forRead = AerospikeReadData.forRead(new Key(NAMESPACE, SIMPLESET, 10L), record(bins));

		CustomTypeWithListAndMap actual = converter.read(CustomTypeWithListAndMap.class, forRead);

		CustomTypeWithListAndMap expected = new CustomTypeWithListAndMap(ImmutableList.of("firstItem",
				of("keyInList", "valueInList"),
				new Address(new Street("Gogolya str.", 15), 567)),
				of("map", of("key", "value")));
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void shouldReadCustomTypeWithCustomType() throws Exception {
		Map<String, Object> bins = of("field", of(
				"listOfObjects", ImmutableList.of("firstItem", of("keyInList", "valueInList")),
				"mapWithObjectValue", of("map", of("key", "value"),
						"address", of("street", of("name", "Gogolya str.", "number", 15, "@_class", Street.class.getName()),
								"apartment", 567, "@_class", Address.class.getName())),
				"@_class", CustomTypeWithCustomType.class.getName(),
				"@user_key", "10"
		));
		AerospikeReadData forRead = AerospikeReadData.forRead(new Key(NAMESPACE, SIMPLESET, 10L), record(bins));

		CustomTypeWithCustomType actual = converter.read(CustomTypeWithCustomType.class, forRead);

		CustomTypeWithCustomType expected = new CustomTypeWithCustomType(new CustomTypeWithListAndMap(ImmutableList.of("firstItem", of("keyInList", "valueInList")),
				of("map", of("key", "value"),
						"address", new Address(new Street("Gogolya str.", 15), 567))));
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void shouldReadListsAndMapsWithObjectImmutable() throws Exception {
		Map<String, Object> bins = of(
				"listOfObjects", ImmutableList.of("firstItem", of("keyInList", "valueInList")),
				"mapWithObjectValue", of("map", of("key", "value")),
				"@_class", CustomTypeWithListAndMapImmutable.class.getName(),
				"@user_key", "10"
		);
		AerospikeReadData forRead = AerospikeReadData.forRead(new Key(NAMESPACE, SIMPLESET, 10L), record(bins));

		CustomTypeWithListAndMapImmutable actual = converter.read(CustomTypeWithListAndMapImmutable.class, forRead);

		CustomTypeWithListAndMapImmutable expected = new CustomTypeWithListAndMapImmutable(ImmutableList.of("firstItem", of("keyInList", "valueInList")),
				of("map", of("key", "value")));
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void shouldReadCustomTypeWithCustomTypeImmutable() throws Exception {
		Map<String, Object> bins = of("field", of(
				"listOfObjects", ImmutableList.of("firstItem", of("keyInList", "valueInList")),
				"mapWithObjectValue", of("map", of("key", "value")),
				"@_class", CustomTypeWithCustomTypeImmutable.class.getName(),
				"@user_key", "10"
		));
		AerospikeReadData forRead = AerospikeReadData.forRead(new Key(NAMESPACE, SIMPLESET, 10L), record(bins));

		CustomTypeWithCustomTypeImmutable actual = converter.read(CustomTypeWithCustomTypeImmutable.class, forRead);

		CustomTypeWithCustomTypeImmutable expected = new CustomTypeWithCustomTypeImmutable(new CustomTypeWithListAndMapImmutable(ImmutableList.of("firstItem", of("keyInList", "valueInList")),
				of("map", of("key", "value"))));
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void shouldReadMapWithNonSimpleValue() throws Exception {
		Map<String, Object> bins = of(
				"mapWithNonSimpleValue", of(
						"key1", of("street", of("name", "Gogolya str.", "number", 15, "@_class", Street.class.getName()),
								"apartment", 567, "@_class", Address.class.getName()),
						"key2", of("street", of("name", "Shakespeare str.", "number", 40, "@_class", Street.class.getName()),
								"apartment", 765, "@_class", Address.class.getName())),
				"@_class", MapWithGenericValue.class.getName(),
				"@user_key", "10"
		);
		AerospikeReadData forRead = AerospikeReadData.forRead(new Key(NAMESPACE, SIMPLESET, 10L), record(bins));

		MapWithGenericValue actual = converter.read(MapWithGenericValue.class, forRead);

		Map<String, Address> map = of("key1", new Address(new Street("Gogolya str.", 15), 567),
				"key2", new Address(new Street("Shakespeare str.", 40), 765));
		MapWithGenericValue<Address> expected = new MapWithGenericValue<>(10L, map);
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void shouldWriteObjectWithSimpleFields() throws Exception {
		Set<String> field9 = set("val1", "val2");
		Set<Set<String>> field10 = set(set("1", "2"), set("3", "4"), set());
		SimpleClass object = new SimpleClass(0, "abyrvalg", 13, 14L, (float) 15, 16.0, true, new Date(8878888),
				TYPES.SECOND, field9, field10, (byte) 1);
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();

		converter.write(object, forWrite);

		assertThat(forWrite.getKey()).consistsOf(NAMESPACE, SIMPLESET, 0);
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("field1", "abyrvalg"),
				new Bin("field2", 13),
				new Bin("field3", 14L),
				new Bin("field4", (float) 15),
				new Bin("field5", 16.0),
				new Bin("field6", true),
				new Bin("field7", 8878888L),
				new Bin("field8", "SECOND"),
				new Bin("field9", list("val2", "val1")),
				new Bin("field10", list(list(), list("1", "2"), list("3", "4"))),
				new Bin("field11", (byte) 1),
//				new Bin("field12", (byte)'d'),//TODO: chars not supported
				new Bin("@_class", "simpleclass"),
				new Bin("@user_key", "0")
		);
	}

	@Test
	public void shouldReadObjectWithSimpleFields() throws Exception {
		Map<String, Object> bins = new HashMap<>();
		bins.put("field1", "abyrvalg");
		bins.put("field2", 13);
		bins.put("field3", 14L);
		bins.put("field4", (float) 15);
		bins.put("field5", 16.0);
		bins.put("field6", true);
		bins.put("field7", 77777L);
		bins.put("field8", "SECOND");
		bins.put("field9", list("val1", "val2"));
		bins.put("field10", list(list(), list("1", "2"), list("3", "4")));
		bins.put("field11", (byte) 1);
		bins.put("field12", (byte) 'd');//TODO: chars not supported
		AerospikeReadData forRead = AerospikeReadData.forRead(new Key(NAMESPACE, SIMPLESET, 867), record(bins));

		SimpleClass actual = converter.read(SimpleClass.class, forRead);

		assertThat(actual).isEqualTo(new SimpleClass(867, "abyrvalg", 13, 14L, (float) 15, 16.0, true,
				new Date(77777L), TYPES.SECOND, set("val1", "val2"), set(set("1", "2"), set("3", "4"), set()),
				(byte) 1));
	}

	@Test
	public void shouldWriteObjectWithPersistenceConstructor() throws Exception {
		SimpleClassWithPersistenceConstructor object = new SimpleClassWithPersistenceConstructor(17, "abyrvalg", 13);
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();

		converter.write(object, forWrite);

		assertThat(forWrite.getKey()).consistsOf(NAMESPACE, SIMPLESET2, 17);
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@user_key", "17"),
				new Bin("@_class", SimpleClassWithPersistenceConstructor.class.getName()),
				new Bin("field1", "abyrvalg"),
				new Bin("field2", 13));
	}

	@Test
	public void shouldReadObjectWithPersistenceConstructor() throws Exception {
		Map<String, Object> bins = new HashMap<>();
		bins.put("field1", "abyrvalg");
		bins.put("field2", 13);
		AerospikeReadData forRead = AerospikeReadData.forRead(new Key(NAMESPACE, SIMPLESET, 555), record(bins));

		SimpleClassWithPersistenceConstructor actual = converter.read(SimpleClassWithPersistenceConstructor.class, forRead);

		assertThat(actual).isEqualTo(new SimpleClassWithPersistenceConstructor(555, "abyrvalg", 13));
	}

	@Test
	public void shouldReadComplexClass() throws Exception {
		Map<String, Object> bins = of(
				"name",
				of("firstName", "Vasya", "lastName", "Pupkin"),
				"address",
				of("street",
						of("name", "Gogolya street", "number", 24),
						"apartment", 777)
		);
		AerospikeReadData data = AerospikeReadData.forRead(new Key(NAMESPACE, SIMPLESET, 555), record(bins));

		User actual = converter.read(User.class, data);

		Name name = new Name("Vasya", "Pupkin");
		Address address = new Address(new Street("Gogolya street", 24), 777);
		User expected = new User(555, name, address);
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void shouldWriteComplexClass() throws Exception {
		Name name = new Name("Vasya", "Pupkin");
		Address address = new Address(new Street("Gogolya street", 24), 777);
		User object = new User(10, name, address);
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();

		converter.write(object, forWrite);

		assertThat(forWrite.getKey()).consistsOf(NAMESPACE, SIMPLESET3, 10);
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@user_key", "10"),
				new Bin("@_class", User.class.getName()),
				new Bin("name",
						of("firstName", "Vasya", "lastName", "Pupkin", "@_class", Name.class.getName())),
				new Bin("address",
						of("street",
								of("name", "Gogolya street", "number", 24, "@_class", Street.class.getName()),
								"apartment", 777, "@_class", Address.class.getName()))
		);
	}

	@Test
	public void shouldWriteIntId() {
		ClassWithIntId value = new ClassWithIntId();
		value.id = 5;
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();

		converter.write(value, forWrite);

		assertThat(forWrite.getKey()).consistsOf(NAMESPACE, "ClassWithIntId", 5);
		assertThat(forWrite.getBins()).contains(new Bin("@user_key", "5"));
	}

	@Test
	public void shouldReadIntId() throws Exception {
		Map<String, Object> data = of();

		ClassWithIntId actual = converter.read(ClassWithIntId.class, AerospikeReadData.forRead(new Key(NAMESPACE, "ClassWithIntId", 5), record(data)));

		ClassWithIntId expected = new ClassWithIntId();
		expected.id = 5;
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void shouldWriteSetWithComplexValue() {
		Set<Address> addresses = set(
				new Address(new Street("Southwark Street", 110), 876),
				new Address(new Street("Finsbury Pavement", 125), 13));
		Person person = new Person("kate-01", addresses);

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(person, forWrite);

		assertThat(forWrite.getKey()).consistsOf(NAMESPACE, "Person", "kate-01");
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@_class", Person.class.getName()),
				new Bin("@user_key", "kate-01"),
				new Bin("addresses", list(
						of("street",
								of("name", "Southwark Street", "number", 110, "@_class", Street.class.getName()),
								"apartment", 876, "@_class", Address.class.getName()),
						of("street",
								of("name", "Finsbury Pavement", "number", 125, "@_class", Street.class.getName()),
								"apartment", 13, "@_class", Address.class.getName())
				)));
	}

	@Test
	public void shouldReadSetWithComplexValue() {
		Map<String, Object> bins = of(
				"addresses", list(
						of("street",
								of("name", "Southwark Street", "number", 110),
								"apartment", 876),
						of("street",
								of("name", "Finsbury Pavement", "number", 125),
								"apartment", 13)
				)
		);
		AerospikeReadData dbObject = AerospikeReadData.forRead(new Key(NAMESPACE, "Person", "kate-01"), record(bins));

		Person result = converter.read(Person.class, dbObject);

		Set<Address> addresses = set(
				new Address(new Street("Southwark Street", 110), 876),
				new Address(new Street("Finsbury Pavement", 125), 13));
		Person person = new Person("kate-01", addresses);
		assertThat(result).isEqualTo(person);
	}

	@Test
	public void shouldWriteEnumProperties() throws Exception {
		List<TYPES> list = list(TYPES.FIRST, TYPES.SECOND);
		EnumSet<TYPES> set = EnumSet.allOf(TYPES.class);
		EnumMap<TYPES, String> map = new EnumMap<TYPES, String>(of(TYPES.FIRST, "a", TYPES.SECOND, "b"));
		EnumProperties object = new EnumProperties(TYPES.SECOND, list, set, map);

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(object, forWrite);

		assertThat(forWrite.getKey()).isNull();
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@_class", EnumProperties.class.getName()),
				new Bin("type", "SECOND"),
				new Bin("list", list("FIRST", "SECOND")),
				new Bin("set", list("FIRST", "SECOND", "THIRD")),
				new Bin("map", of("FIRST", "a", "SECOND", "b"))
		);
	}

	@Test
	public void shouldReadEnumProperties() throws Exception {
		Map<String, Object> bins = of(
				"@_class", EnumProperties.class.getName(),
				"type", "SECOND",
				"list", list("FIRST", "SECOND"),
				"set", list("FIRST", "SECOND", "THIRD"),
				"map", of("FIRST", "a", "SECOND", "b")
		);
		AerospikeReadData dbObject = AerospikeReadData.forRead(new Key(NAMESPACE, "EnumProperties", 10L), record(bins));

		EnumProperties result = converter.read(EnumProperties.class, dbObject);

		List<TYPES> list = list(TYPES.FIRST, TYPES.SECOND);
		EnumSet<TYPES> set = EnumSet.allOf(TYPES.class);
		EnumMap<TYPES, String> map = new EnumMap<TYPES, String>(of(TYPES.FIRST, "a", TYPES.SECOND, "b"));
		EnumProperties expected = new EnumProperties(TYPES.SECOND, list, set, map);
		assertThat(result).isEqualTo(expected);
	}

	@Test
	public void shouldWriteSortedMapWithSimpleValue() throws Exception {
		SortedMap<String, String> map = new TreeMap<>(of("a", "b", "c", "d"));
		SortedMapWithSimpleValue object = new SortedMapWithSimpleValue(map);

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(object, forWrite);

		assertThat(forWrite.getKey()).isNull();
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@_class", SortedMapWithSimpleValue.class.getName()),
				new Bin("map", of("a", "b", "c", "d"))
		);
	}

	@Test
	public void shouldReadSortedMapWithSimpleValue() throws Exception {
		Map<String, Object> bins = of(
				"@_class", SortedMapWithSimpleValue.class.getName(),
				"map", of("a", "b", "c", "d")
		);
		AerospikeReadData dbObject = AerospikeReadData.forRead(new Key(NAMESPACE, "SortedMapWithSimpleValue", 10L), record(bins));

		SortedMapWithSimpleValue result = converter.read(SortedMapWithSimpleValue.class, dbObject);

		SortedMap<String, String> map = new TreeMap<>(of("a", "b", "c", "d"));
		SortedMapWithSimpleValue expected = new SortedMapWithSimpleValue(map);
		assertThat(result).isEqualTo(expected);
	}

	@Test
	public void shouldWriteNestedMapsWithSimpleValue() throws Exception {
		Map<String, Map<String, Map<String, String>>> map = of(
				"level-1", of("level-1-1", of("1", "2")),
				"level-2", of("level-2-2", of("1", "2")));
		NestedMapsWithSimpleValue object = new NestedMapsWithSimpleValue(map);

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(object, forWrite);

		assertThat(forWrite.getKey()).isNull();
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@_class", NestedMapsWithSimpleValue.class.getName()),
				new Bin("nestedMaps", of(
						"level-1", of("level-1-1", of("1", "2")),
						"level-2", of("level-2-2", of("1", "2"))))
		);
	}

	@Test
	public void shouldReadNestedMapsWithSimpleValue() throws Exception {
		Map<String, Object> bins = of(
				"@_class", NestedMapsWithSimpleValue.class.getName(),
				"nestedMaps", of(
						"level-1", of("level-1-1", of("1", "2")),
						"level-2", of("level-2-2", of("1", "2")))
		);
		AerospikeReadData dbObject = AerospikeReadData.forRead(new Key(NAMESPACE, "NestedMapsWithSimpleValue", 10L), record(bins));

		NestedMapsWithSimpleValue result = converter.read(NestedMapsWithSimpleValue.class, dbObject);


		Map<String, Map<String, Map<String, String>>> map = of(
				"level-1", of("level-1-1", of("1", "2")),
				"level-2", of("level-2-2", of("1", "2")));
		NestedMapsWithSimpleValue expected = new NestedMapsWithSimpleValue(map);
		assertThat(result).isEqualTo(expected);
	}

	@Test
	public void shouldWriteGenericType() throws Exception {
		GenericType<GenericType<String>> object = new GenericType("string");

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(object, forWrite);

		assertThat(forWrite.getKey()).isNull();
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@_class", GenericType.class.getName()),
				new Bin("content", "string")
		);
	}

	@Test
	public void shouldReadGenericType() throws Exception {
		Map<String, Object> bins = of(
				"@_class", GenericType.class.getName(),
				"content", "string"
		);
		AerospikeReadData dbObject = AerospikeReadData.forRead(new Key(NAMESPACE, "GenericType", 10L), record(bins));

		GenericType result = converter.read(GenericType.class, dbObject);

		GenericType expected = new GenericType("string");
		assertThat(result).isEqualTo(expected);
	}

	@Test
	public void shouldWriteListOfLists() throws Exception {
		ListOfLists object = new ListOfLists(list(list("a", "b", "c"), list("d", "e"), list()));

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(object, forWrite);

		assertThat(forWrite.getKey()).isNull();
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@_class", ListOfLists.class.getName()),
				new Bin("listOfLists", list(list("a", "b", "c"), list("d", "e"), list()))
		);
	}

	@Test
	public void shouldReadListOfLists() throws Exception {
		Map<String, Object> bins = of(
				"@_class", ListOfLists.class.getName(),
				"listOfLists", list(list("a", "b", "c"), list("d", "e"), list())
		);
		AerospikeReadData dbObject = AerospikeReadData.forRead(new Key(NAMESPACE, "ListOfLists", 10L), record(bins));

		ListOfLists result = converter.read(ListOfLists.class, dbObject);

		ListOfLists expected = new ListOfLists(list(list("a", "b", "c"), list("d", "e"), list()));
		assertThat(result).isEqualTo(expected);
	}

	@Test
	public void shouldWriteListOfMaps() throws Exception {
		ListOfMaps object = new ListOfMaps(list(of("vasya", new Name("Vasya", "Pukin")), of("nastya", new Name("Nastya", "Smirnova"))));

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(object, forWrite);

		assertThat(forWrite.getKey()).isNull();
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@_class", ListOfMaps.class.getName()),
				new Bin("listOfMaps", list(
						of("vasya", of("firstName", "Vasya", "lastName", "Pukin", "@_class", Name.class.getName())),
						of("nastya", of("firstName", "Nastya", "lastName", "Smirnova", "@_class", Name.class.getName()))
				)));
	}

	@Test
	public void shouldReadListOfMaps() throws Exception {
		Map<String, Object> bins = of(
				"@_class", ListOfMaps.class.getName(),
				"listOfMaps", list(
						of("vasya", of("firstName", "Vasya", "lastName", "Pukin", "@_class", Name.class.getName())),
						of("nastya", of("firstName", "Nastya", "lastName", "Smirnova", "@_class", Name.class.getName()))
				)
		);
		AerospikeReadData dbObject = AerospikeReadData.forRead(new Key(NAMESPACE, "ListOfMaps", 10L), record(bins));

		ListOfMaps result = converter.read(ListOfMaps.class, dbObject);

		ListOfMaps expected = new ListOfMaps(list(of("vasya", new Name("Vasya", "Pukin")), of("nastya", new Name("Nastya", "Smirnova"))));
		assertThat(result).isEqualTo(expected);
	}

	@Test
	public void shouldWriteContainerOfCustomFieldNames() throws Exception {
		ContainerOfCustomFieldNames object = new ContainerOfCustomFieldNames("value", new CustomFieldNames(1, "2"));

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(object, forWrite);

		assertThat(forWrite.getKey()).isNull();
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@_class", ContainerOfCustomFieldNames.class.getName()),
				new Bin("property", "value"),
				new Bin("customFieldNames", of("property1", 1, "property2", "2", "@_class", CustomFieldNames.class.getName()))
		);
	}

	@Test
	public void shouldReadContainerOfCustomFieldNames() throws Exception {
		Map<String, Object> bins = of(
				"@_class", ContainerOfCustomFieldNames.class.getName(),
				"property", "value",
				"customFieldNames", of("property1", 1, "property2", "2", "@_class", CustomFieldNames.class.getName())
		);
		AerospikeReadData dbObject = AerospikeReadData.forRead(new Key(NAMESPACE, "ContainerOfCustomFieldNames", 10L), record(bins));

		ContainerOfCustomFieldNames result = converter.read(ContainerOfCustomFieldNames.class, dbObject);

		ContainerOfCustomFieldNames expected = new ContainerOfCustomFieldNames("value", new CustomFieldNames(1, "2"));
		assertThat(result).isEqualTo(expected);
	}

	@Test
	public void shouldWriteClassWithComplexId() throws Exception {
		ClassWithComplexId object = new ClassWithComplexId(new ComplexId(10L));

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(object, forWrite);

		assertThat(forWrite.getKey()).consistsOf(NAMESPACE, ClassWithComplexId.class.getSimpleName(), "id::10");
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@_class", ClassWithComplexId.class.getName()),
				new Bin("@user_key", "id::10")
		);
	}

	@Test
	public void shouldReadClassWithComplexId() throws Exception {
		Map<String, Object> bins = of(
				"@_class", ClassWithComplexId.class.getName(),
				"@user_key", "id::10"
		);
		AerospikeReadData dbObject = AerospikeReadData.forRead(new Key(NAMESPACE, "ClassWithComplexId", 10L), record(bins));

		ClassWithComplexId result = converter.read(ClassWithComplexId.class, dbObject);

		ClassWithComplexId expected = new ClassWithComplexId(new ComplexId(10L));
		assertThat(result).isEqualTo(expected);
	}

	@Test
	public void usesDocumentsStoredTypeIfSubtypeOfRequest() {
		Map<String, Object> bins = of(
				"@_class", Person.class.getName(),
				"addresses", list()
		);
		AerospikeReadData dbObject = AerospikeReadData.forRead(new Key(NAMESPACE, "Person", "kate-01"), record(bins));

		Contact result = converter.read(Contact.class, dbObject);
		assertThat(result).isInstanceOf(Person.class);
	}

	@Test
	public void shouldWriteCollectionOfObjects() throws Exception {
		CollectionOfObjects object = new CollectionOfObjects(list(new Person(null, Collections.emptySet())));

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(object, forWrite);

		assertThat(forWrite.getBins()).containsOnly(
				new Bin("collection", list(of("addresses", list(), "@_class", Person.class.getName()))),
				new Bin("@_class", CollectionOfObjects.class.getName())
		);
	}

	@Test
	public void shouldReadCollectionOfObjects() throws Exception {
		Map<String, Object> bins = of(
				"@_class", CollectionOfObjects.class.getName(),
				"collection", list(of("@_class", Person.class.getName(), "addresses", set()))
		);
		AerospikeReadData dbObject = AerospikeReadData.forRead(new Key(NAMESPACE, "CollectionOfObjects", "any"), record(bins));

		CollectionOfObjects result = converter.read(CollectionOfObjects.class, dbObject);

		assertThat(result).isEqualTo(new CollectionOfObjects(new LinkedHashSet<>(list(new Person(null, Collections.emptySet())))));
	}

	@Test
	public void shouldWriteAndReadUsingCustomConverter() throws Exception {
		MappingAerospikeConverter converter =
				getMappingAerospikeConverter(new UserToAerospikeWriteDataConverter(), new AerospikeReadDataToUserConverter());

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		User user = new User(678, new Name("Nastya", "Smirnova"), null);
		converter.write(user, forWrite);

		assertThat(forWrite.getKey()).consistsOf("custom-namespace", "custom-set", 678L);
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("fs", "Nastya"), new Bin("ls", "Smirnova")
		);

		Map<String, Object> bins = of("fs", "Nastya", "ls", "Smirnova");
		User read = converter.read(User.class, AerospikeReadData.forRead(forWrite.getKey(), record(bins)));

		assertThat(read).isEqualTo(user);
	}

	@Test
	public void shouldWriteAndReadIfTypeKeyIsNull() throws Exception {
		MappingAerospikeConverter converter =
				getMappingAerospikeConverter(new AerospikeTypeAliasAccessor(null));

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		User user = new User(678L, null, null);
		converter.write(user, forWrite);

		assertThat(forWrite.getKey()).consistsOf(NAMESPACE, SIMPLESET3, user.getId());
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@user_key", "678")
		);

		Map<String, Object> bins = of("@user_key", "678");
		User read = converter.read(User.class, AerospikeReadData.forRead(forWrite.getKey(), record(bins)));

		assertThat(read).isEqualTo(user);
	}

	@Test
	public void shouldWriteAndReadIdFieldOfNonDocumentClass() throws Exception {
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		MapWithGenericValue<ClassWithIdField> expected = new MapWithGenericValue<>(788L,
				of("key", new ClassWithIdField(45L, "v")));
		converter.write(expected, forWrite);

		assertThat(forWrite.getKey()).consistsOf(NAMESPACE, MapWithGenericValue.class.getSimpleName(), 788L);
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@_class", MapWithGenericValue.class.getName()),
				new Bin("@user_key", "788"),
				new Bin("mapWithNonSimpleValue",
						of("key", of("id", 45L, "field", "v", "@_class", ClassWithIdField.class.getName())))
		);

		Map<String, Object> bins = of(
				"@_class", MapWithGenericValue.class.getName(),
				"@user_key", "788",
				"mapWithNonSimpleValue",
				of("key", of("id", 45L, "field", "v", "@_class", ClassWithIdField.class.getName())));
		MapWithGenericValue actual = converter.read(MapWithGenericValue.class,
				AerospikeReadData.forRead(forWrite.getKey(), record(bins)));

		assertThat(actual).isEqualTo(expected);

	}

	@Test
	public void shouldWriteExpirationValue() {
		Person person = new Person("personId", Collections.emptySet());
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(person, forWrite);
		assertThat(forWrite.getExpiration()).isEqualTo(EXPIRATION_ONE_SECOND);
	}

	@Test
	public void shouldReadExpirationFieldValue() {
		Key key = new Key(NAMESPACE, "docId", 10L);

		int recordExpiration = toRecordExpiration(EXPIRATION_ONE_MINUTE);
		Record record = new Record(Collections.emptyMap(), 0, recordExpiration);

		AerospikeReadData readData = AerospikeReadData.forRead(key, record);

		DocumentWithExpirationAnnotation forRead = converter.read(DocumentWithExpirationAnnotation.class, readData);
		// Because of converting record expiration to TTL in Record.getTimeToLive method,
		// we may have expected expiration minus one second
		assertThat(forRead.getExpiration()).isIn(EXPIRATION_ONE_MINUTE, EXPIRATION_ONE_MINUTE - 1);
	}

	@Test
	public void shouldReadUnixTimeExpirationFieldValue() {
		Key key = new Key(NAMESPACE, "docId", 10L);
		int recordExpiration = toRecordExpiration(EXPIRATION_ONE_MINUTE);
		Record record = new Record(Collections.emptyMap(), 0, recordExpiration);

		AerospikeReadData readData = AerospikeReadData.forRead(key, record);
		DocumentWithUnixTimeExpiration forRead = converter.read(DocumentWithUnixTimeExpiration.class, readData);

		DateTime actual = forRead.getExpiration();
		DateTime expected = DateTime.now().plusSeconds(EXPIRATION_ONE_MINUTE);
		assertThat(actual.getMillis()).isCloseTo(expected.getMillis(), Offset.offset(100L));
	}

	@Test
	public void shouldWriteUnixTimeExpirationFieldValue() {
		DateTime unixTimeExpiration = DateTime.now().plusSeconds(EXPIRATION_ONE_MINUTE);
		DocumentWithUnixTimeExpiration document = new DocumentWithUnixTimeExpiration("docId", unixTimeExpiration);

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(document, forWrite);

		assertThat(forWrite.getExpiration()).isIn(EXPIRATION_ONE_MINUTE, EXPIRATION_ONE_MINUTE - 1);
	}

	@Test
	public void shouldFailWriteUnixTimeExpirationFieldValue() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Expiration value must be greater than zero");

		DateTime unixTimeExpiration = DateTime.now().minusSeconds(EXPIRATION_ONE_MINUTE);
		DocumentWithUnixTimeExpiration document = new DocumentWithUnixTimeExpiration("docId", unixTimeExpiration);

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(document, forWrite);
	}

	private int toRecordExpiration(int expiration) {
		ZonedDateTime documentExpiration = ZonedDateTime.now(ZoneOffset.UTC).plus(expiration, ChronoUnit.SECONDS);
		ZonedDateTime aerospikeExpirationOffset = ZonedDateTime.of(2010, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
		return (int) Duration.between(aerospikeExpirationOffset, documentExpiration).getSeconds();
	}

	@Test
	public void shouldWriteExpirationFieldValue() {
		DocumentWithExpirationAnnotation document = new DocumentWithExpirationAnnotation("docId", EXPIRATION_ONE_SECOND);
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(document, forWrite);
		assertThat(forWrite.getExpiration()).isEqualTo(EXPIRATION_ONE_SECOND);
	}

	@Test
	public void shouldNotSaveExpirationFieldAsBin() {
		DocumentWithExpirationAnnotation document = new DocumentWithExpirationAnnotation("docId", EXPIRATION_ONE_SECOND);
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(document, forWrite);
		assertThat(forWrite.getBins()).doesNotContain(new Bin("expiration", Value.get(EXPIRATION_ONE_SECOND)));
	}

	@Test
	public void shouldFailWithNullExpirationFieldValue() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Expiration must not be null!");

		DocumentWithExpirationAnnotation document = new DocumentWithExpirationAnnotation("docId", null);
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(document, forWrite);
	}

	@Test
	public void shouldFailWithIllegalExpirationFieldValue() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Expiration value must be greater than zero, but was: -1");

		DocumentWithExpirationAnnotation document = new DocumentWithExpirationAnnotation("docId", -1);
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(document, forWrite);
	}

	@Test
	public void shouldReadExpirationForDocumentWithDefaultConstructor() {
		int recordExpiration = toRecordExpiration(EXPIRATION_ONE_MINUTE);
		Record record = new Record(Collections.emptyMap(), 0, recordExpiration);
		Key key = new Key(NAMESPACE, "DocumentWithDefaultConstructor", "docId");
		AerospikeReadData forRead = AerospikeReadData.forRead(key, record);

		DocumentWithDefaultConstructor document = converter.read(DocumentWithDefaultConstructor.class, forRead);
		DateTime actual = document.getExpiration();
		DateTime expected = DateTime.now().plusSeconds(EXPIRATION_ONE_MINUTE);
		assertThat(actual.getMillis()).isCloseTo(expected.getMillis(), Offset.offset(100L));
	}

	@Test
	public void shouldReadExpirationForDocumentWithPersistenceConstructor() {
		int recordExpiration = toRecordExpiration(EXPIRATION_ONE_MINUTE);
		Record record = new Record(Collections.emptyMap(), 0, recordExpiration);
		Key key = new Key(NAMESPACE, "DocumentWithExpirationAnnotationAndPersistenceConstructor", "docId");
		AerospikeReadData forRead = AerospikeReadData.forRead(key, record);

		DocumentWithExpirationAnnotationAndPersistenceConstructor document = converter.read(DocumentWithExpirationAnnotationAndPersistenceConstructor.class, forRead);
		assertThat(document.getExpiration()).isCloseTo(TimeUnit.MINUTES.toSeconds(1), Offset.offset(100L));
	}

	@Test
	public void shouldNotWriteVersion() throws Exception {
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(new VersionedClass("id", "data", 42L), forWrite);

		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@user_key", "id"),
				new Bin("@_class", VersionedClass.class.getName()),
				new Bin("field", "data")
		);
	}

	@Test
	public void shouldWriteObjectWithByteArrayField() {
		DocumentWithByteArray object = new DocumentWithByteArray("user-id", new byte[]{1, 0, 0, 1, 1, 1, 0, 0});
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();

		converter.write(object, forWrite);

		assertThat(forWrite.getKey()).consistsOf(NAMESPACE, "DocumentWithByteArray", "user-id");
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@user_key", "user-id"),
				new Bin("@_class", DocumentWithByteArray.class.getName()),
				new Bin("array", new byte[]{1, 0, 0, 1, 1, 1, 0, 0}));
	}

	@Test
	public void shouldReadObjectWithByteArrayField() {
		Map<String, Object> bins = new HashMap<>();
		bins.put("array", new byte[]{1, 0, 0, 1, 1, 1, 0, 0});
		AerospikeReadData forRead = AerospikeReadData.forRead(new Key(NAMESPACE, "DocumentWithByteArray", "user-id"), record(bins));

		DocumentWithByteArray actual = converter.read(DocumentWithByteArray.class, forRead);

		assertThat(actual).isEqualTo(new DocumentWithByteArray("user-id", new byte[]{1, 0, 0, 1, 1, 1, 0, 0}));
	}

	@Test
	public void shouldReadAndWriteObjectWithBigDecimal() {
		Map<String, BigDecimal> bigDecimalMap = new HashMap<>();
		bigDecimalMap.put("big-decimal-val", new BigDecimal("767867678687678"));
		List<BigDecimal> bigDecimalList = Arrays.asList(new BigDecimal("988687642340235"));
		BigDecimal bigDecimal = new BigDecimal("999999999999999999999999998746");
		BigDecimalContainer object = new BigDecimalContainer("my-id", bigDecimal, bigDecimalMap, bigDecimalList);
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();

		converter.write(object, forWrite);

		assertThat(forWrite.getKey()).consistsOf(NAMESPACE, "BigDecimalContainer", "my-id");
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@user_key", "my-id"),
				new Bin("@_class", BigDecimalContainer.class.getName()),
				new Bin("collection", list("988687642340235")),
				new Bin("value", "999999999999999999999999998746"),
				new Bin("map", of("big-decimal-val", "767867678687678"))
		);

		AerospikeReadData forRead = AerospikeReadData.forRead(forWrite.getKey(), record(forWrite.getBins()));

		BigDecimalContainer actual = converter.read(BigDecimalContainer.class, forRead);

		assertThat(actual).isEqualTo(object);
	}

	@Test
	public void shouldReadObjectWithByteArrayFieldWithOneValueInData() {
		Map<String, Object> bins = new HashMap<>();
		bins.put("array", 1);
		AerospikeReadData forRead = AerospikeReadData.forRead(new Key(NAMESPACE, "DocumentWithByteArray", "user-id"), record(bins));

		DocumentWithByteArray actual = converter.read(DocumentWithByteArray.class, forRead);

		assertThat(actual).isEqualTo(new DocumentWithByteArray("user-id", new byte[]{1}));
	}

	private static Record record(Collection<Bin> bins) {
		Map<String, Object> collect = bins.stream()
				.collect(Collectors.toMap(bin -> bin.name, bin -> bin.value.getObject()));
		return record(collect);
	}

	private static Record record(Map<String, Object> bins) {
		return new Record(bins, 0, 0);
	}
}
