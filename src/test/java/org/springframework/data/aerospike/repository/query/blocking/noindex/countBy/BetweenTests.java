package org.springframework.data.aerospike.repository.query.blocking.noindex.countBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.noindex.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.util.TestUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the "Is between" repository query. Keywords: Between, IsBetween.
 */
public class BetweenTests extends PersonRepositoryQueryTests {

    @Test
    void countBySimplePropertyBetween_Integer() {
        assertThat(dave.getAge()).isBetween(40, 46);
        long result = repository.countByAgeBetween(40, 46);
        assertThat(result).isGreaterThan(0);
    }

    @Test
    void countBySimplePropertyBetween_String() {
        long result = repository.countByFirstNameBetween("Dave", "David");
        assertThat(result).isGreaterThan(0);
    }

    @Test
    void countByNestedSimplePropertyBetween_Integer() {
        oliver.setFriend(alicia);
        repository.save(oliver);
        dave.setFriend(oliver);
        repository.save(dave);
        carter.setFriend(dave);
        repository.save(carter);

        long result = repository.countByFriendAgeBetween(40, 45);
        assertThat(result).isGreaterThan(0);

        TestUtils.setFriendsToNull(repository, oliver, dave, carter);
    }

    @Test
    void countByNestedSimplePropertyBetween_String() {
        assertThat(carter.getAddress().getZipCode()).isEqualTo("C0124");
        assertThat(dave.getAddress().getZipCode()).isEqualTo("C0123");
        assertThat(repository.countByAddressZipCodeBetween("C0123", "C0124")).isGreaterThan(0);
    }

    /**
     * Collections are converted to Lists when saved to AerospikeDB.
     * <p>
     * Argument of type Collection meant to be compared with a List in DB also gets converted to a List.
     * <p>
     * In this test we are providing a SortedSet and a PriorityQueue which preserve the order of elements,
     * such Collections can be consistently compared to a List saved in DB.
     */
    @Test
    void countByCollectionBetween_SortedSet() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            Set<Integer> davesIntSet = Set.of(1);
            dave.setIntSet(davesIntSet);
            repository.save(dave);
            assertThat(dave.getIntSet()).isEqualTo(davesIntSet);

            Set<Integer> setToCompareWith = new TreeSet<>(Set.of(3, 1, 2, 4, 0)); // gets sorted using natural order
            long persons = repository.countByIntSetBetween(Set.of(0), setToCompareWith);
            assertThat(persons).isZero();

            Set<Integer> setToCompareWith2 = new TreeSet<>(Comparator.reverseOrder());
            setToCompareWith2.addAll(Set.of(3, 1, 2, 4, 0)); // gets sorted using Comparator: 4, 3, 2, 1, 0
            long persons2 = repository.countByIntSetBetween(Set.of(0), setToCompareWith2);
            assertThat(persons2).isGreaterThan(0);

            List<Integer> listToCompareWith = List.of(0, 4, 3, 1, 2); // the insertion order is preserved
            long persons3 = repository.countByIntSetBetween(Set.of(0), listToCompareWith);
            assertThat(persons3).isZero();

            // gets sorted using natural order
            PriorityQueue<Integer> queueToCompareWith = new PriorityQueue<>(Set.of(3, 1, 2, 4, 0));
            long persons4 = repository.countByIntSetBetween(Set.of(0), queueToCompareWith);
            assertThat(persons4).isZero();

            PriorityQueue<Integer> queueToCompareWith2 = new PriorityQueue<>(Comparator.reverseOrder());
            queueToCompareWith2.addAll(Set.of(3, 1, 2, 4, 0)); // gets sorted using Comparator: 4, 3, 2, 1, 0
            long persons5 = repository.countByIntSetBetween(Set.of(0), queueToCompareWith2);
            assertThat(persons5).isGreaterThan(0);
        }
    }

    @Test
    void countByCollectionBetween_IntegerList() {
        List<Integer> list1 = List.of(100, 200, 300);
        List<Integer> list2 = List.of(1000, 2000, 3000);

        long persons = repository.countByIntsBetween(list1, list2);
        assertThat(persons).isGreaterThan(0);
    }

    @Test
    void countByCollectionBetween_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.countByIntsBetween())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.ints BETWEEN: invalid number of arguments, expecting two");

        assertThatThrownBy(() -> negativeTestsRepository.countByIntsBetween(100))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.ints BETWEEN: invalid number of arguments, expecting two");

        assertThatThrownBy(() -> negativeTestsRepository.countByIntsBetween(Map.of(100, 200), Map.of(300, 400)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.ints BETWEEN: invalid argument type, expecting Collection");
    }
}
