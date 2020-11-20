package org.springframework.data.aerospike.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.aerospike.sample.Person;

import java.util.Arrays;
import java.util.List;

import static org.springframework.data.aerospike.AsCollections.of;
import static org.springframework.data.aerospike.utility.AerospikeUniqueId.nextId;

@RequiredArgsConstructor
public class PersonTestData {

    public static Person dave = Person.builder().id(nextId()).firstName("Dave").lastName("Matthews").age(42).strings(Arrays.asList("str1", "str2")).build();
    public static Person donny = Person.builder().id(nextId()).firstName("Donny").lastName("Macintire").age(39).strings(Arrays.asList("str1", "str2", "str3")).build();
    public static Person oliver = Person.builder().id(nextId()).firstName("Oliver August").lastName("Matthews").age(4).build();
    public static Person carter = Person.builder().id(nextId()).firstName("Carter").lastName("Beauford").age(49).build();
    public static Person boyd = Person.builder().id(nextId()).firstName("Boyd").lastName("Tinsley").age(45).map(of("key1", "val1", "key2", "val2")).build();
    public static Person stefan = Person.builder().id(nextId()).firstName("Stefan").lastName("Lessard").age(34).map(of("key1", "val1", "key2", "val2", "key3", "val3")).build();
    public static Person leroi = Person.builder().id(nextId()).firstName("Leroi").lastName("Moore").age(41).build();
    public static Person leroi2 = Person.builder().id(nextId()).firstName("Leroi").lastName("Moore").age(25).ints(Arrays.asList(550, 990)).build();
    public static Person alicia = Person.builder().id(nextId()).firstName("Alicia").lastName("Keys").age(30).ints(Arrays.asList(550, 600, 990)).build();

    public static List<Person> all = Arrays.asList(oliver, dave, donny, carter, boyd, stefan, leroi, leroi2, alicia);

    public static class Indexed {

        public static IndexedPerson dave = IndexedPerson.from(PersonTestData.dave);
        public static IndexedPerson donny = IndexedPerson.from(PersonTestData.donny);
        public static IndexedPerson oliver = IndexedPerson.from(PersonTestData.oliver);
        public static IndexedPerson carter = IndexedPerson.from(PersonTestData.carter);
        public static IndexedPerson boyd = IndexedPerson.from(PersonTestData.boyd);
        public static IndexedPerson stefan = IndexedPerson.from(PersonTestData.stefan);
        public static IndexedPerson leroi = IndexedPerson.from(PersonTestData.leroi);
        public static IndexedPerson leroi2 = IndexedPerson.from(PersonTestData.leroi2);
        public static IndexedPerson alicia = IndexedPerson.from(PersonTestData.alicia);

        public static List<IndexedPerson> all = Arrays.asList(oliver, dave, donny, carter, boyd, stefan, leroi, leroi2, alicia);

    }

}
