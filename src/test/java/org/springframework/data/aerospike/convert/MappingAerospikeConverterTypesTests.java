package org.springframework.data.aerospike.convert;

import com.aerospike.client.Bin;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.SampleClasses;
import org.springframework.data.aerospike.SampleClasses.*;
import org.springframework.data.aerospike.assertions.KeyAssert;
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
import static org.springframework.data.aerospike.AsCollections.list;
import static org.springframework.data.aerospike.AsCollections.of;
import static org.springframework.data.aerospike.AsCollections.set;
import static org.springframework.data.aerospike.SampleClasses.SimpleClass.SIMPLESET;
import static org.springframework.data.aerospike.SampleClasses.SimpleClassWithPersistenceConstructor.SIMPLESET2;
import static org.springframework.data.aerospike.SampleClasses.User.SIMPLESET3;

public class MappingAerospikeConverterTypesTests extends BaseMappingAerospikeConverterTest {

    private final String id = "my-id";

    @Test
    void NullObjectIfAerospikeDataNull() {
        SimpleClass actual = converter.read(SimpleClass.class, null);

        assertThat(actual).isNull();
    }

    @Test
    void IntegerId() {
        DocumentWithIntId object = new DocumentWithIntId(5);

        assertWriteAndRead(object, "DocumentWithIntId", 5,
            new Bin("@_class", DocumentWithIntId.class.getName())
        );
    }

    @Test
    void StringId() {
        DocumentWithStringId object = new DocumentWithStringId("my-amazing-string-id");

        assertWriteAndRead(object, "DocumentWithStringId", "my-amazing-string-id",
            new Bin("@_class", DocumentWithStringId.class.getName())
        );
    }

    @Test
    void SetWithSimpleValue() {
        SetWithSimpleValue object = new SetWithSimpleValue(1L, set("a", "b", "c", null));

        assertWriteAndRead(object, "SetWithSimpleValue", 1L,
            new Bin("collectionWithSimpleValues", list(null, "a", "b", "c")),
            new Bin("@_class", SetWithSimpleValue.class.getName())
        );
    }

    @Test
    void MapWithSimpleValue() {
        Map<String, String> map = of("key1", "value1", "key2", "value2", "key3", null);
        MapWithSimpleValue object = new MapWithSimpleValue(10L, map);

        assertWriteAndRead(object,
            MapWithSimpleValue.class.getSimpleName(), 10L,
            new Bin("mapWithSimpleValue", of("key1", "value1", "key2", "value2", "key3", null)),
            new Bin("@_class", MapWithSimpleValue.class.getName())
        );
    }

    @Test
    void IdClassConverterNotFound() {
        SampleClasses.SomeId someId1 = new SampleClasses.SomeId("partA", "partB1");
        SampleClasses.SomeEntity someEntity1 = new SampleClasses.SomeEntity(someId1, "fieldA", 42L);
        Map<SampleClasses.SomeId, SampleClasses.SomeEntity> entityMap = new HashMap<>();
        entityMap.put(someId1, someEntity1);
        DocumentExample id = new DocumentExample("id", entityMap);
        DocumentExampleIdClass object = new DocumentExampleIdClass(id, 1L, 1234567890, 10L);

        assertThatThrownBy(() -> assertWriteAndRead(object, DocumentExampleIdClass.class.getSimpleName(), 10L,
            new Bin("counter", 1L),
            new Bin("@_version", 1234567890),
            new Bin("update", 10L),
            new Bin("@_class", DocumentExampleIdClass.class.getName())
        ))
            .isInstanceOf(org.springframework.core.convert.ConverterNotFoundException.class)
            .hasMessage("No converter found capable of converting from type " +
                "[org.springframework.data.aerospike.SampleClasses$DocumentExample] to type [java.lang.String]");
    }

    @Test
    void MapWithCollectionValues() {
        Map<String, List<String>> map = of("key1", list(), "key2", list("a", "b", "c"));
        MapWithCollectionValue object = new MapWithCollectionValue(10L, map);

        assertWriteAndRead(object,
            MapWithCollectionValue.class.getSimpleName(), 10L,
            new Bin("mapWithCollectionValue", of("key1", list(), "key2", list("a", "b", "c"))),
            new Bin("@_class", MapWithCollectionValue.class.getName())
        );
    }

    @Test
    void MapWithNonSimpleValue() {
        Map<String, Address> map = of("key1", new Address(new Street("Gogolya str.", 15), 567),
            "key2", new Address(new Street("Shakespeare str.", 40), 765));
        MapWithGenericValue<Address> object = new MapWithGenericValue<>(10L, map);

        assertWriteAndRead(object, MapWithGenericValue.class.getSimpleName(), 10L,
            new Bin("mapWithNonSimpleValue", of(
                "key1", of("street", of("name", "Gogolya str.", "number", 15, "@_class", Street.class.getName()),
                    "apartment", 567, "@_class", Address.class.getName()),
                "key2", of("street", of("name", "Shakespeare str.", "number", 40, "@_class", Street.class.getName()),
                    "apartment", 765, "@_class", Address.class.getName()))),
            new Bin("@_class", MapWithGenericValue.class.getName())
        );
    }

    @Test
    void ListsAndMapsWithObjectValue() {
        CustomTypeWithListAndMap object = new CustomTypeWithListAndMap(id, ImmutableList.of("firstItem",
            of("keyInList", "valueInList"),
            new Address(new Street("Gogolya str.", 15), 567)),
            of("map", of("key", "value")));

        assertWriteAndRead(object, "CustomTypeWithListAndMap", id,
            new Bin("listOfObjects", list("firstItem",
                of("keyInList", "valueInList"),
                of("street", of("name", "Gogolya str.", "number", 15, "@_class", Street.class.getName()),
                    "apartment", 567, "@_class", Address.class.getName()))),
            new Bin("mapWithObjectValue", of("map", of("key", "value"))),
            new Bin("@_class", CustomTypeWithListAndMap.class.getName())
        );
    }

    @Test
    void CustomTypeWithCustomType() {
        CustomTypeWithCustomType object = new CustomTypeWithCustomType(id, new ImmutableListAndMap(
            ImmutableList.of("firstItem", of("keyInList", "valueInList")),
            of("map", of("key", "value"),
                "address", new Address(new Street("Gogolya str.", 15), 567))));

        assertWriteAndRead(object, "CustomTypeWithCustomType", id,
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

    @Test
    void ListsAndMapsWithObjectImmutable() {
        CustomTypeWithListAndMapImmutable object = new CustomTypeWithListAndMapImmutable(id,
            ImmutableList.of("firstItem", of("keyInList", "valueInList")),
            of("map", of("key", "value")));

        assertWriteAndRead(object, "CustomTypeWithListAndMapImmutable", id,
            new Bin("listOfObjects", list("firstItem", of("keyInList", "valueInList"))),
            new Bin("mapWithObjectValue", of("map", of("key", "value"))),
            new Bin("@_class", CustomTypeWithListAndMapImmutable.class.getName())
        );
    }

    @Test
    void ObjectWithSimpleFields() {
        Set<String> field9 = set("val1", "val2");
        Set<Set<String>> field10 = set(set("1", "2"), set("3", "4"), set());
        SimpleClass object = new SimpleClass(777L, "abyrvalg", 13, 14L, (float) 15, 16.0, true, new Date(8878888),
            TYPES.SECOND, field9, field10, (byte) 1, '3', 'd');

        assertWriteAndRead(object, SIMPLESET, 777L,
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

    @Test
    void ObjectWithPersistenceConstructor() {
        SimpleClassWithPersistenceConstructor object = new SimpleClassWithPersistenceConstructor(17, "abyrvalg", 13);

        assertWriteAndRead(object, SIMPLESET2, 17,
            new Bin("@_class", SimpleClassWithPersistenceConstructor.class.getName()),
            new Bin("field1", "abyrvalg"),
            new Bin("field2", 13));
    }

    @Test
    void ComplexClass() {
        Name name = new Name("Vasya", "Pupkin");
        Address address = new Address(new Street("Gogolya street", 24), 777);
        User object = new User(10, name, address);

        assertWriteAndRead(object, SIMPLESET3, 10,
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
    void SetWithComplexValue() {
        Set<Address> addresses = set(
            new Address(new Street("Southwark Street", 110), 876),
            new Address(new Street("Finsbury Pavement", 125), 13));
        Person object = new Person("kate-01", addresses);

        assertWriteAndRead(object, "Person", "kate-01",
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

    @Test
    void EnumProperties() {
        List<TYPES> list = list(TYPES.FIRST, TYPES.SECOND);
        EnumSet<TYPES> set = EnumSet.allOf(TYPES.class);
        EnumMap<TYPES, String> map = new EnumMap<TYPES, String>(of(TYPES.FIRST, "a", TYPES.SECOND, "b"));
        ClassWithEnumProperties object = new ClassWithEnumProperties("id", TYPES.SECOND, list, set, map);

        assertWriteAndRead(object, "ClassWithEnumProperties", "id",
            new Bin("@_class", ClassWithEnumProperties.class.getName()),
            new Bin("type", "SECOND"),
            new Bin("list", list("FIRST", "SECOND")),
            new Bin("set", list("FIRST", "SECOND", "THIRD")),
            new Bin("map", of("FIRST", "a", "SECOND", "b"))
        );
    }

    @Test
    void SortedMapWithSimpleValue() {
        SortedMap<String, String> map = new TreeMap<>(of("a", "b", "c", "d"));
        SortedMapWithSimpleValue object = new SortedMapWithSimpleValue(id, map);

        assertWriteAndRead(object, "SortedMapWithSimpleValue", id,
            new Bin("@_class", SortedMapWithSimpleValue.class.getName()),
            new Bin("map", of("a", "b", "c", "d"))
        );
    }

    @Test
    void NestedMapsWithSimpleValue() {
        Map<String, Map<String, Map<String, String>>> map = of(
            "level-1", of("level-1-1", of("1", "2")),
            "level-2", of("level-2-2", of("1", "2")));
        NestedMapsWithSimpleValue object = new NestedMapsWithSimpleValue(id, map);

        assertWriteAndRead(object, "NestedMapsWithSimpleValue", id,
            new Bin("@_class", NestedMapsWithSimpleValue.class.getName()),
            new Bin("nestedMaps", of(
                "level-1", of("level-1-1", of("1", "2")),
                "level-2", of("level-2-2", of("1", "2"))))
        );
    }

    @Test
    void GenericType() {
        //noinspection rawtypes
        @SuppressWarnings("unchecked") GenericType<GenericType<String>> object = new GenericType(id, "string");

        assertWriteAndRead(object, "GenericType", id,
            new Bin("@_class", GenericType.class.getName()),
            new Bin("content", "string")
        );
    }

    @Test
    void ListOfLists() {
        ListOfLists object = new ListOfLists(id, list(list("a", "b", "c"), list("d", "e"), list()));

        assertWriteAndRead(object, "ListOfLists", id,
            new Bin("@_class", ListOfLists.class.getName()),
            new Bin("listOfLists", list(list("a", "b", "c"), list("d", "e"), list()))
        );
    }

    @Test
    void ListOfMaps() {
        ListOfMaps object = new ListOfMaps(id, list(of("vasya", new Name("Vasya", "Pukin")), of("nastya",
            new Name("Nastya", "Smirnova"))));

        assertWriteAndRead(object, "ListOfMaps", id,
            new Bin("@_class", ListOfMaps.class.getName()),
            new Bin("listOfMaps", list(
                of("vasya", of("firstName", "Vasya", "lastName", "Pukin", "@_class", Name.class.getName())),
                of("nastya", of("firstName", "Nastya", "lastName", "Smirnova", "@_class", Name.class.getName()))
            )));
    }

    @Test
    void ContainerOfCustomFieldNames() {
        ContainerOfCustomFieldNames object = new ContainerOfCustomFieldNames(id, "value", new CustomFieldNames(1
            , "2"));

        assertWriteAndRead(object, "ContainerOfCustomFieldNames", id,
            new Bin("@_class", ContainerOfCustomFieldNames.class.getName()),
            new Bin("property", "value"),
            new Bin("customFieldNames", of("property1", 1, "property2", "2", "@_class",
                CustomFieldNames.class.getName()))
        );
    }

    @Test
    void ClassWithComplexId() {
        ClassWithComplexId object = new ClassWithComplexId(new ComplexId(10L));

        assertWriteAndRead(object, ClassWithComplexId.class.getSimpleName(), "id::10",
            new Bin("@_class", ClassWithComplexId.class.getName())
        );
    }

    @Test
    void IdFieldOfNonDocumentClass() {
        MapWithGenericValue<ClassWithIdField> object = new MapWithGenericValue<>(788L,
            of("key", new ClassWithIdField(45L, "v")));

        assertWriteAndRead(object, MapWithGenericValue.class.getSimpleName(), 788L,
            new Bin("@_class", MapWithGenericValue.class.getName()),
            new Bin("mapWithNonSimpleValue",
                of("key", of("id", 45L, "field", "v", "@_class", ClassWithIdField.class.getName())))
        );
    }

    @Test
    void ObjectWithByteArrayField() {
        DocumentWithByteArray object = new DocumentWithByteArray(id, new byte[]{1, 0, 0, 1, 1, 1, 0, 0});

        assertWriteAndRead(object,
            "DocumentWithByteArray", id,
            new Bin("@_class", DocumentWithByteArray.class.getName()),
            new Bin("array", new byte[]{1, 0, 0, 1, 1, 1, 0, 0}));
    }

    @Test
    void ObjectWithArrayField() {
        int[] array = new int[]{1, 0, 0, 1, 1, 1, 0, 0};
        DocumentWithIntArray object = new DocumentWithIntArray(id, array);

        assertWriteAndRead(object,
            "DocumentWithIntArray", id,
            new Bin("@_class", DocumentWithIntArray.class.getName()),
            new Bin("array", Arrays.stream(array).boxed().toList()));
    }

    @Test
    void ObjectWithAtomicField() {
        AtomicInteger atomicInteger = new AtomicInteger(10);
        AtomicLong atomicLong = new AtomicLong(10L);
        DocumentWithAtomicFields object = new DocumentWithAtomicFields(id, atomicInteger, atomicLong);

        DocumentWithAtomicFields readDoc = readObjectAfterWriting(object,
            "DocumentWithAtomicFields", id,
            new Bin("@_class", DocumentWithAtomicFields.class.getName()),
            new Bin("atomicInteger", AerospikeConverters.AtomicIntegerToIntegerConverter.INSTANCE.convert(atomicInteger)),
            new Bin("atomicLong", AerospikeConverters.AtomicLongToLongConverter.INSTANCE.convert(atomicLong)));


        assertThat(readDoc.getId()).isEqualTo(object.getId());
        assertThat(readDoc.getAtomicInteger().intValue()).isEqualTo(object.getAtomicInteger().intValue());
        assertThat(readDoc.getAtomicLong().longValue()).isEqualTo(object.getAtomicLong().longValue());
    }

    @Test
    void ObjectWithURLField() {
        URL url;
        try {
            url = new URL("http://example.com");
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        DocumentWithURL object = new DocumentWithURL(id, url);

        assertWriteAndRead(object,
            "DocumentWithURL", id,
            new Bin("@_class", DocumentWithURL.class.getName()),
            new Bin("url", AerospikeConverters.URLToStringConverter.INSTANCE.convert(url)));
    }

    @Test
    void ObjectWithUUIDField() {
        UUID uuid = new UUID(10L, 5L);
        DocumentWithUUID object = new DocumentWithUUID(id, uuid);

        assertWriteAndRead(object,
            "DocumentWithUUID", id,
            new Bin("@_class", DocumentWithUUID.class.getName()),
            new Bin("uuid", AerospikeConverters.UuidToStringConverter.INSTANCE.convert(uuid)));
    }

    @Test
    void ObjectWithCurrencyField() {
        Currency currency = Currency.getInstance("USD");
        DocumentWithCurrency object = new DocumentWithCurrency(id, currency);

        assertWriteAndRead(object,
            "DocumentWithCurrency", id,
            new Bin("@_class", DocumentWithCurrency.class.getName()),
            new Bin("currency", AerospikeConverters.CurrencyToStringConverter.INSTANCE.convert(currency)));
    }

    @Test
    void ObjectWithDateField() {
        Date date = Date.from(Instant.now());
        DocumentWithDate object = new DocumentWithDate(id, date);

        assertWriteAndRead(object,
            "DocumentWithDate", id,
            new Bin("@_class", DocumentWithDate.class.getName()),
            new Bin("date", DateConverters.DateToLongConverter.INSTANCE.convert(date)));
    }

    @Test
    void ObjectWithCalendarField() {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("PTC"));
        calendar.setTime(Date.from(Instant.now()));
        DocumentWithCalendar object = new DocumentWithCalendar(id, calendar);

        assertWriteAndRead(object,
            "DocumentWithCalendar", id,
            new Bin("@_class", DocumentWithCalendar.class.getName()),
            new Bin("calendar", DateConverters.CalendarToMapConverter.INSTANCE.convert(calendar)));
    }

    @Test
    void ObjectWithDurationField() {
        Duration duration = Duration.ofSeconds(12345678910L);
        DocumentWithDuration object = new DocumentWithDuration(id, duration);

        assertWriteAndRead(object,
            "DocumentWithDuration", id,
            new Bin("@_class", DocumentWithDuration.class.getName()),
            new Bin("duration", DateConverters.DurationToStringConverter.INSTANCE.convert(duration)));
    }

    @Test
    void ObjectWithBigDecimal() {
        Map<String, BigDecimal> bigDecimalMap = new HashMap<>();
        bigDecimalMap.put("big-decimal-val", new BigDecimal("767867678687678"));
        List<BigDecimal> bigDecimalList = List.of(new BigDecimal("988687642340235"));
        BigDecimal bigDecimal = new BigDecimal("999999999999999999999999998746");
        BigDecimalContainer object = new BigDecimalContainer(id, bigDecimal, bigDecimalMap, bigDecimalList);

        assertWriteAndRead(object,
            "BigDecimalContainer", id,
            new Bin("@_class", BigDecimalContainer.class.getName()),
            new Bin("collection", list("988687642340235")),
            new Bin("value", "999999999999999999999999998746"),
            new Bin("map", of("big-decimal-val", "767867678687678"))
        );
    }

    @Test
    void ObjectWithByteArrayFieldWithOneValueInData() {
        DocumentWithByteArray object = new DocumentWithByteArray(id, new byte[]{1});

        assertWriteAndRead(object, "DocumentWithByteArray", id,
            new Bin("@_class", DocumentWithByteArray.class.getName()),
            new Bin("array", new byte[]{1})
        );
    }

    @Test
    void shouldWriteAsArrayListAndReadAsByteArray() {
        MappingAerospikeConverter converter =
            getMappingAerospikeConverter(new AerospikeTypeAliasAccessor(null));

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
            getMappingAerospikeConverter(new AerospikeTypeAliasAccessor(null));

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

    private <T> void assertWriteAndRead(T object,
                                        String expectedSet,
                                        Object expectedUserKey,
                                        Bin... expectedBins) {

        AerospikeWriteData forWrite = AerospikeWriteData.forWrite(NAMESPACE);

        converter.write(object, forWrite);

        KeyAssert.assertThat(forWrite.getKey()).consistsOf(NAMESPACE, expectedSet, expectedUserKey);
        assertThat(forWrite.getBins()).containsOnly(expectedBins);

        AerospikeReadData forRead = AerospikeReadData.forRead(forWrite.getKey(), aeroRecord(forWrite.getBins()));

        @SuppressWarnings("unchecked") T actual = (T) converter.read(object.getClass(), forRead);

        assertThat(actual).isEqualTo(object);
    }

    private <T> T readObjectAfterWriting(T object,
                                        String expectedSet,
                                        Object expectedUserKey,
                                        Bin... expectedBins) {

        AerospikeWriteData forWrite = AerospikeWriteData.forWrite(NAMESPACE);

        converter.write(object, forWrite);

        KeyAssert.assertThat(forWrite.getKey()).consistsOf(NAMESPACE, expectedSet, expectedUserKey);
        assertThat(forWrite.getBins()).containsOnly(expectedBins);

        AerospikeReadData forRead = AerospikeReadData.forRead(forWrite.getKey(), aeroRecord(forWrite.getBins()));

        @SuppressWarnings("unchecked") T actual = (T) converter.read(object.getClass(), forRead);

        return actual;
    }
}
