package org.springframework.data.aerospike.repository.query.blocking.noindex.find;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.noindex.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.util.TestUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is greater than or equal" repository query. Keywords: GreaterThanEqual, IsGreaterThanEqual.
 */
public class GreaterThanOrEqualTests extends PersonRepositoryQueryTests {

    @Test
    void findByNestedSimplePropertyGreaterThan_String() {
        String zipCode = "C0124";
        assertThat(carter.getAddress().getZipCode()).isEqualTo(zipCode);
        assertThat(repository.findByAddressZipCodeGreaterThanEqual(zipCode)).containsExactly(carter);
    }

    /**
     * Collections are converted to Lists when saved to AerospikeDB.
     * <p>
     * Argument of type Collection meant to be compared with a List in DB also gets converted to a List.
     * <p>
     * In this test we are providing an unordered Collection (Set) which means that the order of elements in a resulting
     * List cannot be guaranteed.
     * <p>
     * Comparing with an unordered Collection works only for equality (EQ/NOTEQ) operations and not for
     * LT/LTEQ/GT/GTEQ/BETWEEN.
     */
    @Test
    void findByCollectionGreaterThanOrEqual_UnorderedSet_Equals_List() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            Set<Integer> setToCompareWith = Set.of(0, 1, 2, 3, 4);
            dave.setIntSet(setToCompareWith);
            repository.save(dave);
            assertThat(dave.getIntSet()).isEqualTo(setToCompareWith);

            List<Person> persons = repository.findByIntSetGreaterThanEqual(setToCompareWith);
            assertThat(persons).contains(dave);
        }
    }

    /**
     * Collections are converted to Lists when saved to AerospikeDB.
     * <p>
     * Argument of type Collection meant to be compared with a List in DB also gets converted to a List.
     * <p>
     * In this test we are providing a SortedSet and a PriorityQueue which preserve the order of elements, such
     * Collections can be consistently compared to a List saved in DB.
     */
    @Test
    void findByCollectionGreaterThanOrEqual_SortedSet() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            Set<Integer> davesIntSet = Set.of(1);
            dave.setIntSet(davesIntSet);
            repository.save(dave);
            assertThat(dave.getIntSet()).isEqualTo(davesIntSet);

            Set<Integer> setToCompareWith = new TreeSet<>(Set.of(0, 1, 2, 3, 4)); // natural order
            List<Person> persons = repository.findByIntSetGreaterThanEqual(setToCompareWith);
            assertThat(persons).contains(dave);

            Set<Integer> setToCompareWith2 = new TreeSet<>(Comparator.comparingInt(Integer::intValue));
            setToCompareWith2.addAll(Set.of(3, 1, 2, 4, 0)); // gets sorted using Comparator
            List<Person> persons2 = repository.findByIntSetGreaterThanEqual(setToCompareWith2);
            assertThat(persons2).contains(dave);

            List<Integer> listToCompareWith = List.of(3, 1, 2, 0, 4); // the insertion order is preserved
            List<Person> persons3 = repository.findByIntSetGreaterThanEqual(listToCompareWith);
            assertThat(persons3).doesNotContain(dave);

            // gets sorted using natural order
            PriorityQueue<Integer> queueToCompareWith = new PriorityQueue<>(Set.of(3, 1, 2, 4, 0));
            List<Person> persons4 = repository.findByIntSetGreaterThanEqual(queueToCompareWith);
            assertThat(persons4).contains(dave);
        }
    }

    @Test
    void findByCollectionGreaterThanOrEqual_List() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            List<Integer> listToCompareWith = List.of(1, 2);
            dave.setInts(listToCompareWith);
            repository.save(dave);
            assertThat(dave.getInts()).isEqualTo(listToCompareWith);

            List<Person> persons = repository.findByIntsGreaterThanEqual(List.of(1, 1, 1, 1, 1, 1, 10));
            assertThat(persons).contains(dave);
        }
    }

    @Test
    void findByNestedCollectionGreaterThanOrEqual() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            dave.setInts(List.of(1, 2, 3, 4));
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendIntsGreaterThanEqual(List.of(1, 2, 3, 4));

            assertThat(result).contains(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    void findByNestedMapGreaterThanOrEqual() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            dave.setIntMap(Map.of("1", 2, "3", 4));
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendIntMapGreaterThanEqual(Map.of("1", 2, "3", 4));

            assertThat(result).contains(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    void findByNestedPojoGreaterThanOrEqual() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            Address address = new Address("Foo Street 1", 1, "C0123", "Bar");
            assertThat(carter.getAddress()).isNotNull();

            dave.setFriend(carter);
            repository.save(dave);

            List<Person> result = repository.findByFriendAddressGreaterThanEqual(address);

            assertThat(result).contains(dave);
            TestUtils.setFriendsToNull(repository, dave);
        }
    }
}
