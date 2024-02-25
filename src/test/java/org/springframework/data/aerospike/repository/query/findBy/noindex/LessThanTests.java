package org.springframework.data.aerospike.repository.query.findBy.noindex;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the "Is less than" repository query. Keywords: LessThan, IsLessThan.
 */
public class LessThanTests extends PersonRepositoryQueryTests {

    @Test
    void findBySimpleProperty_Integer_Unpaged() {
        Page<Person> page = repository.findByAgeLessThan(40, Pageable.unpaged());
        assertThat(page.hasContent()).isTrue();
        assertThat(page.getNumberOfElements()).isGreaterThan(1);
        assertThat(page.hasNext()).isFalse();
        assertThat(page.getTotalPages()).isEqualTo(1);
        assertThat(page.getTotalElements()).isEqualTo(page.getSize());
    }

    @Test
    void findByCollectionLessThan() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            List<String> davesStrings = dave.getStrings();
            List<String> listToCompareWith = List.of("str1", "str2", "str3");
            List<String> listWithFewerElements = List.of("str1", "str2");

            dave.setStrings(listWithFewerElements);
            repository.save(dave);
            assertThat(donny.getStrings()).isEqualTo(listToCompareWith);
            assertThat(dave.getStrings()).isEqualTo(listWithFewerElements);

            List<Person> persons = repository.findByStringsLessThan(listToCompareWith);
            assertThat(persons).contains(dave);

            dave.setStrings(davesStrings);
            repository.save(dave);
        }
    }

    @Test
    void findPersonsByCollectionLessThan_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByStringsLessThan())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.strings LT: invalid number of arguments, expecting one");

        assertThatThrownBy(() -> negativeTestsRepository.findByStringsLessThan(List.of("string1"), List.of(
            "String2")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.strings LT: invalid number of arguments, expecting one");

        assertThatThrownBy(() -> negativeTestsRepository.findByStringsLessThan("string1", "string2"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.strings LT: invalid number of arguments, expecting one");
    }

    @Test
    void findByExactMapKeyWithValueLessThanPOJO() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            Map<String, Address> mapOfAddresses1 = Map.of("a", new Address("Foo Street 1", 1, "C0123", "Bar"));
            Map<String, Address> mapOfAddresses2 = Map.of("b", new Address("Foo Street 2", 1, "C0123", "Bar"));
            Map<String, Address> mapOfAddresses3 = Map.of("c", new Address("Foo Street 2", 1, "C0124", "Bar"));
            Map<String, Address> mapOfAddresses4 = Map.of("d", new Address("Foo Street 1234", 1, "C01245", "Bar"));
            stefan.setAddressesMap(mapOfAddresses1);
            repository.save(stefan);
            douglas.setAddressesMap(mapOfAddresses2);
            repository.save(douglas);
            matias.setAddressesMap(mapOfAddresses3);
            repository.save(matias);
            leroi2.setAddressesMap(mapOfAddresses4);
            repository.save(leroi2);

            List<Person> persons;
            persons = repository.findByAddressesMapLessThan("a", new Address("Foo Street 1", 1, "C0124", "Bar"));
            assertThat(persons).containsOnly(stefan);

            persons = repository.findByAddressesMapLessThan("b", new Address("Foo Street 3", 1, "C0123", "Bar"));
            assertThat(persons).containsOnly(douglas);

            persons = repository.findByAddressesMapLessThan("c", new Address("Foo Street 3", 2, "C0124", "Bar"));
            assertThat(persons).containsOnly(matias);

            persons = repository.findByAddressesMapLessThan("d", new Address("Foo Street 1234", 1, "C01245", "Bar"));
            assertThat(persons).isEmpty();

            stefan.setAddressesMap(null);
            repository.save(stefan);
            douglas.setAddressesMap(null);
            repository.save(douglas);
            matias.setAddressesMap(null);
            repository.save(matias);
            leroi2.setAddressesMap(null);
            repository.save(leroi2);
        }
    }

    @Test
    void findByMapLessThanNegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByIntMapLessThan(100))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.intMap LT: invalid combination of arguments, expecting one (Map) or two (Map key and " +
                "value)");

        assertThatThrownBy(() -> negativeTestsRepository.findByIntMapLessThan(100, 200, 300))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.intMap LT: invalid number of arguments, expecting one (Map) or two (Map key and " +
                "value)");

        assertThatThrownBy(() -> negativeTestsRepository.findByIntMapLessThan(new Person("id1", "name1"), 400))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.intMap LT: invalid first argument type, expected String, Number or byte[]");
    }

    @Test
    void findByPOJOLessThan() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            Address address = new Address("Foo Street 2", 2, "C0124", "C0123");
            assertThat(dave.getAddress()).isNotEqualTo(address);
            assertThat(carter.getAddress()).isEqualTo(address);

            List<Person> persons = repository.findByAddressLessThan(address);
            assertThat(persons).containsExactlyInAnyOrder(dave, boyd);
        }
    }
}
