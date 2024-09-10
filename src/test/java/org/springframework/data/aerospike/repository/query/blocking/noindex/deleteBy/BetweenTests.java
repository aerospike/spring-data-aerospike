package org.springframework.data.aerospike.repository.query.blocking.noindex.deleteBy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.noindex.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.util.TestUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the "Is between" repository query. Keywords: Between, IsBetween.
 */
public class BetweenTests extends PersonRepositoryQueryTests {

    @BeforeEach
    public void beforeEach() {
        additionalAerospikeTestOperations.deleteAllAndVerify(Person.class);
        additionalAerospikeTestOperations.saveAll(repository, allPersons);
    }

    @Test
    void deleteBySimplePropertyBetween_Integer() {
        assertThat(dave.getAge()).isBetween(40, 46);
        assertThat(repository.findByAgeBetween(40, 46)).isNotEmpty();

        repository.deleteByAgeBetween(40, 46);
        assertThat(repository.findByAgeBetween(40, 46)).isEmpty();
    }

    @Test
    void deleteBySimplePropertyBetween_String() {
        assertThat(repository.findByFirstNameBetween("Dave", "David")).isNotEmpty();

        repository.deleteByFirstNameBetween("Dave", "David");
        assertThat(repository.findByFirstNameBetween("Dave", "David")).isEmpty();
    }

    @Test
    void deleteByNestedSimplePropertyBetween_Integer() {
        oliver.setFriend(alicia);
        repository.save(oliver);
        dave.setFriend(oliver);
        repository.save(dave);
        carter.setFriend(dave);
        repository.save(carter);
        assertThat(repository.findByFriendAgeBetween(40, 45)).isNotEmpty();

        repository.deleteByFriendAgeBetween(40, 45);
        assertThat(repository.findByFriendAgeBetween(40, 45)).isEmpty();

        TestUtils.setFriendsToNull(repository, oliver, dave, carter);
    }

    @Test
    void deleteByNestedSimplePropertyBetween_String() {
        assertThat(carter.getAddress().getZipCode()).isEqualTo("C0124");
        assertThat(dave.getAddress().getZipCode()).isEqualTo("C0123");
        assertThat(repository.findByAddressZipCodeBetween("C0123", "C0124")).isNotEmpty();

        repository.deleteByAddressZipCodeBetween("C0123", "C0124");
        assertThat(repository.findByAddressZipCodeBetween("C0123", "C0124")).isEmpty();
    }

    /**
     * Collections are converted to Lists when saved to AerospikeDB.
     * <p>
     * Argument of type Collection meant to be compared with a List in DB also gets converted to a List.
     * <p>
     * In this test we are providing a SortedSet which preserves the order of elements,
     * such Collections can be consistently compared to a List saved in DB.
     */
    @Test
    void deleteByCollectionBetween_SortedSet() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            Set<Integer> davesIntSet = Set.of(1);
            dave.setIntSet(davesIntSet);
            repository.save(dave);
            assertThat(dave.getIntSet()).isEqualTo(davesIntSet);

            Set<Integer> setToCompareWith = new TreeSet<>(Set.of(3, 1, 2, 4, 0)); // gets sorted using natural order
            assertThat(repository.findByIntSetBetween(Set.of(0), setToCompareWith)).isEmpty();

            repository.deleteByIntSetBetween(Set.of(0), setToCompareWith);
            assertThat(repository.findByIntSetBetween(Set.of(0), setToCompareWith)).isEmpty();

            Set<Integer> setToCompareWith2 = new TreeSet<>(Comparator.reverseOrder());
            setToCompareWith2.addAll(Set.of(3, 1, 2, 4, 0)); // gets sorted using Comparator: 4, 3, 2, 1, 0
            assertThat(repository.findByIntSetBetween(Set.of(0), setToCompareWith2)).isNotEmpty();

            repository.deleteByIntSetBetween(Set.of(0), setToCompareWith2);
            assertThat(repository.findByIntSetBetween(Set.of(0), setToCompareWith2)).isEmpty();
        }
    }

    @Test
    void deleteByCollectionBetween_IntegerList() {
        List<Integer> list1 = List.of(100, 200, 300);
        List<Integer> list2 = List.of(1000, 2000, 3000);
        assertThat(repository.findByIntsBetween(list1, list2)).isNotEmpty();

        repository.deleteByIntsBetween(list1, list2);
        assertThat(repository.findByIntsBetween(list1, list2)).isEmpty();
    }

    @Test
    void deleteByCollectionBetween_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.deleteByIntsBetween())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.ints BETWEEN: invalid number of arguments, expecting two");

        assertThatThrownBy(() -> negativeTestsRepository.deleteByIntsBetween(100))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.ints BETWEEN: invalid number of arguments, expecting two");

        assertThatThrownBy(() -> negativeTestsRepository.deleteByIntsBetween(Map.of(100, 200), Map.of(300, 400)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.ints BETWEEN: invalid argument type, expecting Collection");
    }
}
