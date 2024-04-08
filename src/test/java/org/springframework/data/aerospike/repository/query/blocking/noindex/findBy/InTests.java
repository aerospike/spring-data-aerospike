package org.springframework.data.aerospike.repository.query.blocking.noindex.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.noindex.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.util.TestUtils;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is in" repository query. Keywords: In, IsIn.
 */
public class InTests extends PersonRepositoryQueryTests {

    @Test
    void findBySimplePropertyIn_String() {
        Stream<Person> result;
        result = repository.findByFirstNameIn(List.of("Anastasiia", "Daniil"));
        assertThat(result).isEmpty();

        result = repository.findByFirstNameIn(List.of("Alicia", "Stefan"));
        assertThat(result).contains(alicia, stefan);
    }

    @Test
    void findByNestedCollectionIn() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            dave.setInts(List.of(1, 2, 3, 4));
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendIntsIn(List.of(List.of(0, 1, 2, 3, 4, 5, 6, 7),
                List.of(1, 2, 3), List.of(1, 2, 3, 4)));

            assertThat(result).contains(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }
}
