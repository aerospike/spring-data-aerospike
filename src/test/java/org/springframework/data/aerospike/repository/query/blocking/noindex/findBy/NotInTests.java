package org.springframework.data.aerospike.repository.query.blocking.noindex.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.noindex.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.util.TestUtils;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is not in" repository query. Keywords: NotIn, IsNotIn.
 */
public class NotInTests extends PersonRepositoryQueryTests {

    @Test
    void findBySimplePropertyNotIn_String() {
        Collection<String> firstNames;
        firstNames = allPersons.stream().map(Person::getFirstName).collect(Collectors.toSet());
        assertThat(repository.findByFirstNameNotIn(firstNames)).isEmpty();

        firstNames = List.of("Dave", "Donny", "Carter", "Boyd", "Leroi", "Stefan", "Matias", "Douglas");
        assertThat(repository.findByFirstNameNotIn(firstNames)).containsExactlyInAnyOrder(oliver, alicia);
    }

    @Test
    void findByNestedCollectionNotIn() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            dave.setInts(List.of(1, 2, 3, 4));
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendIntsNotIn(List.of(List.of(0, 1, 2, 3, 4, 5, 6, 7),
                List.of(1, 2, 3), List.of(1, 2, 3, 4)));

            assertThat(result).contains(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }
}
