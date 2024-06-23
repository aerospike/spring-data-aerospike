package org.springframework.data.aerospike.convert;

import com.aerospike.client.Bin;
import com.aerospike.client.command.ParticleType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.aerospike.assertions.KeyAssert;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.sample.SampleClasses;
import org.springframework.data.aerospike.sample.SampleClasses.*;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.sample.SampleClasses.SimpleClass.SIMPLESET;
import static org.springframework.data.aerospike.sample.SampleClasses.SimpleClassWithPersistenceConstructor.SIMPLESET2;
import static org.springframework.data.aerospike.sample.SampleClasses.User.SIMPLESET3;
import static org.springframework.data.aerospike.util.AsCollections.list;
import static org.springframework.data.aerospike.util.AsCollections.of;
import static org.springframework.data.aerospike.util.AsCollections.set;

public class MappingAerospikeConverterTypesTests extends BaseMappingAerospikeConverterTest {

    private final String id = "my-id";

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void nullObjectIfAerospikeDataNull(int converterOption) {
        SimpleClass actual = getAerospikeMappingConverterByOption(converterOption).read(SimpleClass.class, null);

        assertThat(actual).isNull();
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void primitiveShortId(int converterOption) {
        DocumentWithPrimitiveShortId object = new DocumentWithPrimitiveShortId((short) 5);

        assertWriteAndRead(converterOption, object, "DocumentWithPrimitiveShortId", (short) 5,
            new Bin("@_class", DocumentWithPrimitiveShortId.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void primitiveIntId(int converterOption) {
        DocumentWithPrimitiveIntId object = new DocumentWithPrimitiveIntId(5);

        assertWriteAndRead(converterOption, object, "DocumentWithPrimitiveIntId", 5,
            new Bin("@_class", DocumentWithPrimitiveIntId.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void primitiveLongId(int converterOption) {
        DocumentWithPrimitiveLongId object = new DocumentWithPrimitiveLongId(5L);

        assertWriteAndRead(converterOption, object, "DocumentWithPrimitiveLongId", 5L,
            new Bin("@_class", DocumentWithPrimitiveLongId.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void primitiveCharId(int converterOption) {
        DocumentWithPrimitiveCharId object = new DocumentWithPrimitiveCharId('a');

        assertWriteAndRead(converterOption, object, "DocumentWithPrimitiveCharId", 'a',
            new Bin("@_class", DocumentWithPrimitiveCharId.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void primitiveByteId(int converterOption) {
        DocumentWithPrimitiveByteId object = new DocumentWithPrimitiveByteId((byte) 100);

        assertWriteAndRead(converterOption, object, "DocumentWithPrimitiveByteId",
            (byte) 100, new Bin("@_class", DocumentWithPrimitiveByteId.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void shortId(int converterOption) {
        DocumentWithShortId object = DocumentWithShortId.builder().id((short) 5).build();

        assertWriteAndRead(converterOption, object, "DocumentWithShortId", (short) 5,
            new Bin("@_class", DocumentWithShortId.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void integerId(int converterOption) {
        DocumentWithIntegerId object = DocumentWithIntegerId.builder().id(5).build();

        assertWriteAndRead(converterOption, object, "DocumentWithIntegerId", 5,
            new Bin("@_class", DocumentWithIntegerId.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void longId(int converterOption) {
        DocumentWithLongId object = DocumentWithLongId.builder().id(5L).build();

        assertWriteAndRead(converterOption, object, "DocumentWithLongId", 5L,
            new Bin("@_class", DocumentWithLongId.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void characterId(int converterOption) {
        DocumentWithCharacterId object = DocumentWithCharacterId.builder().id('a').build();

        assertWriteAndRead(converterOption, object, "DocumentWithCharacterId", 'a',
            new Bin("@_class", DocumentWithCharacterId.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void byteId(int converterOption) {
        DocumentWithByteId object = DocumentWithByteId.builder().id(((byte) 100)).build();

        assertWriteAndRead(converterOption, object, "DocumentWithByteId", (byte) 100,
            new Bin("@_class", DocumentWithByteId.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void stringId(int converterOption) {
        DocumentWithStringId object = DocumentWithStringId.builder().id("my-amazing-string-id").build();

        assertWriteAndRead(converterOption, object, "DocumentWithStringId",
            "my-amazing-string-id", new Bin("@_class", DocumentWithStringId.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void byteArrayId(int converterOption) {
        DocumentWithByteArrayId object = DocumentWithByteArrayId.builder()
            .id((new byte[]{1, 0, 0, 1, 1, 1, 0, 0}))
            .build();

        assertWriteAndRead(converterOption, object, "DocumentWithByteArrayId",
            new byte[]{1, 0, 0, 1, 1, 1, 0, 0}, new Bin("@_class", DocumentWithByteArrayId.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void setWithSimpleValue(int converterOption) {
        SetWithSimpleValue object = new SetWithSimpleValue(1L, set("a", "b", "c", null));

        assertWriteAndRead(converterOption, object, "SetWithSimpleValue", 1L,
            new Bin("collectionWithSimpleValues", list(null, "a", "b", "c")),
            new Bin("@_class", SetWithSimpleValue.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void mapWithShortId(int converterOption) {
        Map<Short, String> map = of((short) 1, "value1", (short) 2, "value2", (short) 3, null);
        MapWithShortId object = new MapWithShortId(10L, map);

        assertWriteAndRead(converterOption, object,
            MapWithShortId.class.getSimpleName(), 10L,
            new Bin("mapWithShortId", of((short) 1, "value1", (short) 2, "value2", (short) 3, null)),
            new Bin("@_class", MapWithShortId.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void mapWithIntegerId(int converterOption) {
        Map<Integer, String> map = of(1, "value1", 2, "value2", 3, null);
        MapWithIntegerId object = new MapWithIntegerId(10L, map);

        assertWriteAndRead(converterOption, object,
            MapWithIntegerId.class.getSimpleName(), 10L,
            new Bin("mapWithIntId", of(1, "value1", 2, "value2", 3, null)),
            new Bin("@_class", MapWithIntegerId.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void mapWithLongId(int converterOption) {
        Map<Long, String> map = of(1L, "value1", 2L, "value2", 3L, null);
        MapWithLongId object = new MapWithLongId(10L, map);

        assertWriteAndRead(converterOption, object,
            MapWithLongId.class.getSimpleName(), 10L,
            new Bin("mapWithLongId", of(1L, "value1", 2L, "value2", 3L, null)),
            new Bin("@_class", MapWithLongId.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void mapWithDoubleId(int converterOption) {
        Map<Double, String> map = of(100.25, "value1", 200.25, "value2", 300.25, null);
        MapWithDoubleId object = new MapWithDoubleId(10L, map);

        assertWriteAndRead(converterOption, object,
            MapWithDoubleId.class.getSimpleName(), 10L,
            new Bin("mapWithDoubleId", of(100.25, "value1", 200.25, "value2", 300.25, null)),
            new Bin("@_class", MapWithDoubleId.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void mapWithByteId(int converterOption) {
        Map<Byte, String> map = of((byte) 100, "value1", (byte) 200, "value2", (byte) 300, null);
        MapWithByteId object = new MapWithByteId(10L, map);

        assertWriteAndRead(converterOption, object,
            MapWithByteId.class.getSimpleName(), 10L,
            new Bin("mapWithByteId", of((byte) 100, "value1", (byte) 200, "value2", (byte) 300, null)),
            new Bin("@_class", MapWithByteId.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void mapWithCharacterId(int converterOption) {
        Map<Character, String> map = of('a', "value1", 'b', "value2", 'c', null);
        MapWithCharacterId object = new MapWithCharacterId(10L, map);

        assertWriteAndRead(converterOption, object,
            MapWithCharacterId.class.getSimpleName(), 10L,
            new Bin("mapWithCharacterId", of('a', "value1", 'b', "value2", 'c', null)),
            new Bin("@_class", MapWithCharacterId.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void mapWithSimpleValue(int converterOption) {
        Map<String, String> map = of("key1", "value1", "key2", "value2", "key3", null);
        MapWithStringValue object = new MapWithStringValue(10L, map);

        assertWriteAndRead(converterOption, object,
            MapWithStringValue.class.getSimpleName(), 10L,
            new Bin("mapWithStringValue", of("key1", "value1", "key2", "value2", "key3", null)),
            new Bin("@_class", MapWithStringValue.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void idClassConverterNotFound(int converterOption) {
        SampleClasses.SomeId someId1 = new SampleClasses.SomeId("partA", "partB1");
        SampleClasses.SomeEntity someEntity1 = new SampleClasses.SomeEntity(someId1, "fieldA", 42L);
        Map<SampleClasses.SomeId, SampleClasses.SomeEntity> entityMap = new HashMap<>();
        entityMap.put(someId1, someEntity1);
        DocumentExample id = new DocumentExample("id", entityMap);
        DocumentExampleIdClass object = new DocumentExampleIdClass(id, 1L, 1234567890, 10L);

        assertThatThrownBy(() -> assertWriteAndRead(converterOption, object,
            DocumentExampleIdClass.class.getSimpleName(), 10L,
            new Bin("counter", 1L),
            new Bin("@_version", 1234567890),
            new Bin("update", 10L),
            new Bin("@_class", DocumentExampleIdClass.class.getName())
        ))
            .isInstanceOf(org.springframework.core.convert.ConverterNotFoundException.class)
            .hasMessage("No converter found capable of converting from type " +
                "[org.springframework.data.aerospike.sample.SampleClasses$DocumentExample] to type [java.lang.String]");
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void mapWithCollectionValues(int converterOption) {
        Map<String, List<String>> map = of("key1", list(), "key2", list("a", "b", "c"));
        MapWithCollectionValue object = new MapWithCollectionValue(10L, map);

        assertWriteAndRead(converterOption, object,
            MapWithCollectionValue.class.getSimpleName(), 10L,
            new Bin("mapWithCollectionValue", of("key1", list(), "key2", list("a", "b", "c"))),
            new Bin("@_class", MapWithCollectionValue.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void mapWithNonSimpleValue(int converterOption) {
        Map<String, Address> map = of("key1", new Address(new Street("Gogolya str.", 15), 567),
            "key2", new Address(new Street("Shakespeare str.", 40), 765));
        MapWithGenericValue<Address> object = new MapWithGenericValue<>(10L, map);

        assertWriteAndRead(converterOption, object, MapWithGenericValue.class.getSimpleName(), 10L,
            new Bin("mapWithNonSimpleValue", of(
                "key1", of("street", of("name", "Gogolya str.", "number", 15, "@_class", Street.class.getName()),
                    "apartment", 567, "@_class", Address.class.getName()),
                "key2", of("street", of("name", "Shakespeare str.", "number", 40, "@_class", Street.class.getName()),
                    "apartment", 765, "@_class", Address.class.getName()))),
            new Bin("@_class", MapWithGenericValue.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void listsAndMapsWithObjectValue(int converterOption) {
        CustomTypeWithListAndMap object = new CustomTypeWithListAndMap(id, ImmutableList.of("firstItem",
            of("keyInList", "valueInList"),
            new Address(new Street("Gogolya str.", 15), 567)),
            of("map", of("key", "value")));

        assertWriteAndRead(converterOption, object, "CustomTypeWithListAndMap", id,
            new Bin("listOfObjects", list("firstItem",
                of("keyInList", "valueInList"),
                of("street", of("name", "Gogolya str.", "number", 15, "@_class", Street.class.getName()),
                    "apartment", 567, "@_class", Address.class.getName()))),
            new Bin("mapWithObjectValue", of("map", of("key", "value"))),
            new Bin("@_class", CustomTypeWithListAndMap.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void customTypeWithCustomType(int converterOption) {
        CustomTypeWithCustomType object = new CustomTypeWithCustomType(id, new ImmutableListAndMap(
            ImmutableList.of("firstItem", of("keyInList", "valueInList")),
            of("map", of("key", "value"),
                "address", new Address(new Street("Gogolya str.", 15), 567))));

        assertWriteAndRead(converterOption, object, "CustomTypeWithCustomType", id,
            new Bin("field", of(
                "@_class", ImmutableListAndMap.class.getName(),
                "listOfObjects", list("firstItem", of("keyInList", "valueInList")),
                "mapWithObjectValue", of("map", of("key", "value"),
                    "address", of("street", of("name", "Gogolya str.", "number", 15, "@_class", Street.class.getName()),
                        "apartment", 567, "@_class", Address.class.getName()))
            )),
            new Bin("@_class", CustomTypeWithCustomType.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void listsAndMapsWithObjectImmutable(int converterOption) {
        CustomTypeWithListAndMapImmutable object = new CustomTypeWithListAndMapImmutable(id,
            ImmutableList.of("firstItem", of("keyInList", "valueInList")),
            of("map", of("key", "value")));

        assertWriteAndRead(converterOption, object, "CustomTypeWithListAndMapImmutable", id,
            new Bin("listOfObjects", list("firstItem", of("keyInList", "valueInList"))),
            new Bin("mapWithObjectValue", of("map", of("key", "value"))),
            new Bin("@_class", CustomTypeWithListAndMapImmutable.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void objectWithSimpleFields(int converterOption) {
        Set<String> field9 = set("val1", "val2");
        Set<Set<String>> field10 = set(set("1", "2"), set("3", "4"), set());
        SimpleClass object = new SimpleClass(777L, "abyrvalg", 13, 14L, (float) 15, 16.0, true, new Date(8878888),
            TYPES.SECOND, field9, field10, (byte) 1, '3', 'd');

        assertWriteAndRead(converterOption, object, SIMPLESET, 777L,
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
            new Bin("field12", '3'),
            new Bin("field13", 'd'),
            new Bin("@_class", "simpleclass")
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void objectWithPersistenceConstructor(int converterOption) {
        SimpleClassWithPersistenceConstructor object = new SimpleClassWithPersistenceConstructor(17, "abyrvalg", 13);

        assertWriteAndRead(converterOption, object, SIMPLESET2, 17,
            new Bin("@_class", SimpleClassWithPersistenceConstructor.class.getName()),
            new Bin("field1", "abyrvalg"),
            new Bin("field2", 13));
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void complexClass(int converterOption) {
        Name name = new Name("Vasya", "Pupkin");
        Address address = new Address(new Street("Gogolya street", 24), 777);
        User object = new User(10, name, address);

        assertWriteAndRead(converterOption, object, SIMPLESET3, 10,
            new Bin("@_class", User.class.getName()),
            new Bin("name",
                of("firstName", "Vasya", "lastName", "Pupkin", "@_class", Name.class.getName())),
            new Bin("address",
                of("street",
                    of("name", "Gogolya street", "number", 24, "@_class", Street.class.getName()),
                    "apartment", 777, "@_class", Address.class.getName()))
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void setWithComplexValue(int converterOption) {
        Set<Address> addresses = set(
            new Address(new Street("Southwark Street", 110), 876),
            new Address(new Street("Finsbury Pavement", 125), 13));
        Person object = new Person("kate-01", addresses);

        assertWriteAndRead(converterOption, object, "Person", "kate-01",
            new Bin("@_class", Person.class.getName()),
            new Bin("addresses", list(
                of("street",
                    of("name", "Southwark Street", "number", 110, "@_class", Street.class.getName()),
                    "apartment", 876, "@_class", Address.class.getName()),
                of("street",
                    of("name", "Finsbury Pavement", "number", 125, "@_class", Street.class.getName()),
                    "apartment", 13, "@_class", Address.class.getName())
            )));
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void enumProperties(int converterOption) {
        List<TYPES> list = list(TYPES.FIRST, TYPES.SECOND);
        EnumSet<TYPES> set = EnumSet.allOf(TYPES.class);
        EnumMap<TYPES, String> map = new EnumMap<TYPES, String>(of(TYPES.FIRST, "a", TYPES.SECOND, "b"));
        ClassWithEnumProperties object = new ClassWithEnumProperties("id", TYPES.SECOND, list, set, map);

        assertWriteAndRead(converterOption, object, "ClassWithEnumProperties", "id",
            new Bin("@_class", ClassWithEnumProperties.class.getName()),
            new Bin("type", "SECOND"),
            new Bin("list", list("FIRST", "SECOND")),
            new Bin("set", list("FIRST", "SECOND", "THIRD")),
            new Bin("map", of("FIRST", "a", "SECOND", "b"))
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void sortedMapWithSimpleValue(int converterOption) {
        SortedMap<String, String> map = new TreeMap<>(of("a", "b", "c", "d"));
        SortedMapWithSimpleValue object = new SortedMapWithSimpleValue(id, map);

        assertWriteAndRead(converterOption, object, "SortedMapWithSimpleValue", id,
            new Bin("@_class", SortedMapWithSimpleValue.class.getName()),
            new Bin("map", of("a", "b", "c", "d"))
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void nestedMapsWithSimpleValue(int converterOption) {
        Map<String, Map<String, Map<String, String>>> map = of(
            "level-1", of("level-1-1", of("1", "2")),
            "level-2", of("level-2-2", of("1", "2")));
        NestedMapsWithSimpleValue object = new NestedMapsWithSimpleValue(id, map);

        assertWriteAndRead(converterOption, object, "NestedMapsWithSimpleValue", id,
            new Bin("@_class", NestedMapsWithSimpleValue.class.getName()),
            new Bin("nestedMaps", of(
                "level-1", of("level-1-1", of("1", "2")),
                "level-2", of("level-2-2", of("1", "2"))))
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void genericType(int converterOption) {
        //noinspection rawtypes
        @SuppressWarnings("unchecked") GenericType<GenericType<String>> object = new GenericType(id, "string");

        assertWriteAndRead(converterOption, object, "GenericType", id,
            new Bin("@_class", GenericType.class.getName()),
            new Bin("content", "string")
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void listOfLists(int converterOption) {
        ListOfLists object = new ListOfLists(id, list(list("a", "b", "c"), list("d", "e"), list()));

        assertWriteAndRead(converterOption, object, "ListOfLists", id,
            new Bin("@_class", ListOfLists.class.getName()),
            new Bin("listOfLists", list(list("a", "b", "c"), list("d", "e"), list()))
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void listOfMaps(int converterOption) {
        ListOfMaps object = new ListOfMaps(id, list(of("vasya", new Name("Vasya", "Pukin")), of("nastya",
            new Name("Nastya", "Smirnova"))));

        assertWriteAndRead(converterOption, object, "ListOfMaps", id,
            new Bin("@_class", ListOfMaps.class.getName()),
            new Bin("listOfMaps", list(
                of("vasya", of("firstName", "Vasya", "lastName", "Pukin", "@_class", Name.class.getName())),
                of("nastya", of("firstName", "Nastya", "lastName", "Smirnova", "@_class", Name.class.getName()))
            )));
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void containerOfCustomFieldNames(int converterOption) {
        ContainerOfCustomFieldNames object = new ContainerOfCustomFieldNames(id, "value", new CustomFieldNames(1
            , "2"));

        assertWriteAndRead(converterOption, object, "ContainerOfCustomFieldNames", id,
            new Bin("@_class", ContainerOfCustomFieldNames.class.getName()),
            new Bin("property", "value"),
            new Bin("customFieldNames", of("property1", 1, "property2", "2", "@_class",
                CustomFieldNames.class.getName()))
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void classWithComplexId(int converterOption) {
        ClassWithComplexId object = new ClassWithComplexId(new ComplexId(10L));

        assertWriteAndRead(converterOption, object, ClassWithComplexId.class.getSimpleName(), "id::10",
            new Bin("@_class", ClassWithComplexId.class.getName())
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void idFieldOfNonDocumentClass(int converterOption) {
        MapWithGenericValue<DocumentWithLongId> object = new MapWithGenericValue<>(788L,
            of("key", new DocumentWithLongId(45L, "v")));

        assertWriteAndRead(converterOption, object, MapWithGenericValue.class.getSimpleName(), 788L,
            new Bin("@_class", MapWithGenericValue.class.getName()),
            new Bin("mapWithNonSimpleValue",
                of("key", of("id", 45L, "content", "v", "@_class", DocumentWithLongId.class.getName())))
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void objectWithByteArrayField(int converterOption) {
        DocumentWithByteArray object = new DocumentWithByteArray(id, new byte[]{1, 0, 0, 1, 1, 1, 0, 0});

        assertWriteAndRead(converterOption, object,
            "DocumentWithByteArray", id,
            new Bin("@_class", DocumentWithByteArray.class.getName()),
            new Bin("array", new byte[]{1, 0, 0, 1, 1, 1, 0, 0}));
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void objectWithArrayField(int converterOption) {
        int[] array = new int[]{1, 0, 0, 1, 1, 1, 0, 0};
        DocumentWithIntArray object = new DocumentWithIntArray(id, array);

        assertWriteAndRead(converterOption, object,
            "DocumentWithIntArray", id,
            new Bin("@_class", DocumentWithIntArray.class.getName()),
            new Bin("array", Arrays.stream(array).boxed().toList()));
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void objectWithAtomicField(int converterOption) {
        AtomicInteger atomicInteger = new AtomicInteger(10);
        AtomicLong atomicLong = new AtomicLong(10L);
        DocumentWithAtomicFields object = new DocumentWithAtomicFields(id, atomicInteger, atomicLong);

        DocumentWithAtomicFields readDoc = readObjectAfterWriting(converterOption, object,
            "DocumentWithAtomicFields", id,
            new Bin("@_class", DocumentWithAtomicFields.class.getName()),
            new Bin("atomicInteger",
                AerospikeConverters.AtomicIntegerToIntegerConverter.INSTANCE.convert(atomicInteger)),
            new Bin("atomicLong", AerospikeConverters.AtomicLongToLongConverter.INSTANCE.convert(atomicLong)));


        assertThat(readDoc.getId()).isEqualTo(object.getId());
        assertThat(readDoc.getAtomicInteger().intValue()).isEqualTo(object.getAtomicInteger().intValue());
        assertThat(readDoc.getAtomicLong().longValue()).isEqualTo(object.getAtomicLong().longValue());
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void objectWithURLField(int converterOption) {
        URL url;
        try {
            url = new URL("http://example.com");
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        DocumentWithURL object = new DocumentWithURL(id, url);

        assertWriteAndRead(converterOption, object,
            "DocumentWithURL", id,
            new Bin("@_class", DocumentWithURL.class.getName()),
            new Bin("url", AerospikeConverters.URLToStringConverter.INSTANCE.convert(url)));
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void objectWithUUIDField(int converterOption) {
        UUID uuid = new UUID(10L, 5L);
        DocumentWithUUID object = new DocumentWithUUID(id, uuid);

        assertWriteAndRead(converterOption, object,
            "DocumentWithUUID", id,
            new Bin("@_class", DocumentWithUUID.class.getName()),
            new Bin("uuid", AerospikeConverters.UuidToStringConverter.INSTANCE.convert(uuid)));
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void objectWithCurrencyField(int converterOption) {
        Currency currency = Currency.getInstance("USD");
        DocumentWithCurrency object = new DocumentWithCurrency(id, currency);

        assertWriteAndRead(converterOption, object,
            "DocumentWithCurrency", id,
            new Bin("@_class", DocumentWithCurrency.class.getName()),
            new Bin("currency", AerospikeConverters.CurrencyToStringConverter.INSTANCE.convert(currency)));
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void objectWithDateField(int converterOption) {
        Date date = Date.from(Instant.now());
        DocumentWithDate object = new DocumentWithDate(id, date);

        assertWriteAndRead(converterOption, object,
            "DocumentWithDate", id,
            new Bin("@_class", DocumentWithDate.class.getName()),
            new Bin("date", DateConverters.DateToLongConverter.INSTANCE.convert(date)));
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void objectWithCalendarField(int converterOption) {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("PTC"));
        calendar.setTime(Date.from(Instant.now()));
        DocumentWithCalendar object = new DocumentWithCalendar(id, calendar);

        assertWriteAndRead(converterOption, object,
            "DocumentWithCalendar", id,
            new Bin("@_class", DocumentWithCalendar.class.getName()),
            new Bin("calendar", DateConverters.CalendarToMapConverter.INSTANCE.convert(calendar)));
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void objectWithDurationField(int converterOption) {
        Duration duration = Duration.ofSeconds(12345678910L);
        DocumentWithDuration object = new DocumentWithDuration(id, duration);

        assertWriteAndRead(converterOption, object,
            "DocumentWithDuration", id,
            new Bin("@_class", DocumentWithDuration.class.getName()),
            new Bin("duration", DateConverters.DurationToStringConverter.INSTANCE.convert(duration)));
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void objectWithBigDecimal(int converterOption) {
        Map<String, BigDecimal> bigDecimalMap = new HashMap<>();
        bigDecimalMap.put("big-decimal-val", new BigDecimal("767867678687678"));
        List<BigDecimal> bigDecimalList = List.of(new BigDecimal("988687642340235"));
        BigDecimal bigDecimal = new BigDecimal("999999999999999999999999998746");
        BigDecimalContainer object = new BigDecimalContainer(id, bigDecimal, bigDecimalMap, bigDecimalList);

        assertWriteAndRead(converterOption, object,
            "BigDecimalContainer", id,
            new Bin("@_class", BigDecimalContainer.class.getName()),
            new Bin("collection", list("988687642340235")),
            new Bin("value", "999999999999999999999999998746"),
            new Bin("map", of("big-decimal-val", "767867678687678"))
        );
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1})
    void objectWithByteArrayFieldWithOneValueInData(int converterOption) {
        DocumentWithByteArray object = new DocumentWithByteArray(id, new byte[]{1});

        assertWriteAndRead(converterOption, object, "DocumentWithByteArray", id,
            new Bin("@_class", DocumentWithByteArray.class.getName()),
            new Bin("array", new byte[]{1})
        );
    }

    @Test
    void shouldWriteAsArrayListAndReadAsByteArray() {
        MappingAerospikeConverter converter =
            getMappingAerospikeConverter(settings, new AerospikeTypeAliasAccessor(null));

        AerospikeWriteData forWrite = AerospikeWriteData.forWrite(NAMESPACE);
        DocumentWithByteArrayList docToWrite = new DocumentWithByteArrayList("user-id", Arrays.asList((byte) 1,
            (byte) 2, (byte) 3));
        converter.write(docToWrite, forWrite);

        assertThat(forWrite.getBins()).containsOnly(
            new Bin("array", Arrays.asList((byte) 1, (byte) 2, (byte) 3))
        );

        AerospikeReadData forRead = AerospikeReadData.forRead(forWrite.getKey(), aeroRecord(forWrite.getBins()));
        DocumentWithByteArray actual = converter.read(DocumentWithByteArray.class, forRead);

        assertThat(actual).isEqualTo(new DocumentWithByteArray("user-id", new byte[]{1, 2, 3}));
    }

    @Test
    void shouldWriteAsByteArrayAndReadAsArrayList() {
        MappingAerospikeConverter converter =
            getMappingAerospikeConverter(settings, new AerospikeTypeAliasAccessor(null));

        AerospikeWriteData forWrite = AerospikeWriteData.forWrite(NAMESPACE);
        DocumentWithByteArray docToWrite = new DocumentWithByteArray("user-id", new byte[]{1, 2, 3});
        converter.write(docToWrite, forWrite);

        assertThat(forWrite.getBins()).containsOnly(
            new Bin("array", new byte[]{1, 2, 3})
        );

        AerospikeReadData forRead = AerospikeReadData.forRead(forWrite.getKey(), aeroRecord(forWrite.getBins()));
        DocumentWithByteArrayList actual = converter.read(DocumentWithByteArrayList.class, forRead);

        assertThat(actual).isEqualTo(new DocumentWithByteArrayList("user-id", Arrays.asList((byte) 1, (byte) 2,
            (byte) 3)));
    }

    @Test
    void shouldWriteAndReadNestedPOJOs() {
        MappingAerospikeConverter converter =
            getMappingAerospikeConverter(settings, new AerospikeTypeAliasAccessor(null));

        AerospikeWriteData forWrite = AerospikeWriteData.forWrite(NAMESPACE);
        List<Address> addressesList = List.of(
            new Address(new Street("Street1", 1), 1),
            new Address(new Street("Street2", 2), 2)
        );
        IdAndAddressesList testObj = new IdAndAddressesList("testId", addressesList);
        converter.write(testObj, forWrite);

        assertThat(forWrite.getBins()).containsOnly(
            new Bin("addresses", List.of(
                Map.of("apartment", 1, "street", Map.of("name", "Street1", "number", 1)),
                Map.of("apartment", 2, "street", Map.of("name", "Street2", "number", 2))
            ))
        );

        AerospikeReadData forRead = AerospikeReadData.forRead(forWrite.getKey(), aeroRecord(forWrite.getBins()));
        IdAndAddressesList actual = converter.read(IdAndAddressesList.class, forRead);

        assertThat(actual).isEqualTo(new IdAndAddressesList("testId", addressesList));
    }

    private <T> void assertWriteAndRead(int converterOption,
                                        T object,
                                        String expectedSet,
                                        Object expectedUserKey,
                                        Bin... expectedBins) {
        MappingAerospikeConverter aerospikeConverter = getAerospikeMappingConverterByOption(converterOption);
        AerospikeWriteData forWrite = AerospikeWriteData.forWrite(NAMESPACE);

        aerospikeConverter.write(object, forWrite);

        KeyAssert.assertThat(forWrite.getKey()).consistsOf(aerospikeConverter.getAerospikeDataSettings(), NAMESPACE,
            expectedSet, expectedUserKey);

        for (Bin expectedBin : expectedBins) {
            if (expectedBin.value.getType() == ParticleType.MAP) {
                // Compare Maps
                assertThat(
                    compareMaps(aerospikeConverter.getAerospikeDataSettings(), expectedBin,
                        forWrite.getBins().stream().filter(bin -> bin.name.equals(expectedBin.name))
                            .findFirst().orElse(null))).isTrue();
            } else {
                assertThat(forWrite.getBins()).contains(expectedBin);
            }
        }

        AerospikeReadData forRead = AerospikeReadData.forRead(forWrite.getKey(), aeroRecord(forWrite.getBins()));

        @SuppressWarnings("unchecked") T actual = (T) aerospikeConverter.read(object.getClass(), forRead);

        assertThat(actual).isEqualTo(object);
    }

    private boolean compareMaps(AerospikeDataSettings settings, Bin expected, Bin actual) {
        if (settings != null && settings.isKeepOriginalKeyTypes()) {
            return expected.equals(actual);
        } else {
            // String type is used for unsupported Aerospike key types and previously for all key types in older
            // versions of Spring Data Aerospike
            return Objects.requireNonNull(((Map<?, ?>) actual.value.getObject()).keySet().stream()
                .findFirst().orElse(null)).getClass().equals(String.class);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private <T> T readObjectAfterWriting(int converterOption,
                                         T object,
                                         String expectedSet,
                                         Object expectedUserKey,
                                         Bin... expectedBins) {
        MappingAerospikeConverter aerospikeConverter = getAerospikeMappingConverterByOption(converterOption);
        AerospikeWriteData forWrite = AerospikeWriteData.forWrite(NAMESPACE);

        aerospikeConverter.write(object, forWrite);

        KeyAssert.assertThat(forWrite.getKey()).consistsOf(aerospikeConverter.getAerospikeDataSettings(), NAMESPACE,
            expectedSet, expectedUserKey);
        assertThat(forWrite.getBins()).containsOnly(expectedBins);

        AerospikeReadData forRead = AerospikeReadData.forRead(forWrite.getKey(), aeroRecord(forWrite.getBins()));

        @SuppressWarnings("unchecked") T actual = (T) aerospikeConverter.read(object.getClass(), forRead);

        return actual;
    }
}
