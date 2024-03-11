package org.springframework.data.aerospike.repository.query.findBy.noindex;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.util.List;

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
            assertThat(dave.getAddress().getStreet()).isEqualTo("Foo Street 1");
            assertThat(dave.getAddress().getApartment()).isEqualTo(1);
            assertThat(boyd.getAddress().getStreet()).isEqualTo(null);
            assertThat(boyd.getAddress().getApartment()).isEqualTo(null);

            Address address = new Address("Foo Street 2", 2, "C0124", "C0123");
            assertThat(dave.getAddress()).isNotEqualTo(address);
            assertThat(boyd.getAddress()).isNotEqualTo(address);
            assertThat(carter.getAddress()).isEqualTo(address);

            List<Person> persons = repository.findByAddressLessThan(address);
            assertThat(persons).containsExactlyInAnyOrder(dave, boyd);
        }
    }
}
