package org.springframework.data.aerospike.repository.query.blocking.noindex.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.noindex.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.utility.TestUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the "Starts with" repository query. Keywords: StartingWith, IsStartingWith, StartsWith.
 */
public class StartsWithTests extends PersonRepositoryQueryTests {

    @Test
    void findBySimplePropertyStartingWith_String() {
        List<Person> result = repository.findByFirstNameStartsWith("D");

        assertThat(result).containsOnly(dave, donny, douglas);
    }

    @Test
    void findDistinctByStringSimplePropertyStartingWith() {
        List<Person> persons = repository.findDistinctByFirstNameStartingWith("Leroi");
        assertThat(persons).hasSize(1);
    }

    @Test
    void findBySimplePropertyStartingWith_String_Limited() {
        Person person = repository.findFirstByLastNameStartingWith("M", Sort.by("lastName").ascending());
        assertThat(person).isEqualTo(donny);

        List<Person> personList = repository.findTopByLastNameStartingWith("M", Sort.by("lastName").ascending());
        assertThat(personList).hasSize(1);
        assertThat(personList.get(0)).isEqualTo(person);

        Person person2 = repository.findFirstByLastNameStartingWith("M", Sort.by("age").descending());
        assertThat(person2).isEqualTo(leroi);

        List<Person> persons = repository.findTop3ByLastNameStartingWith("M", Sort.by("lastName", "firstName")
            .ascending());
        List<Person> persons2 = repository.findFirst3ByLastNameStartingWith("M", Sort.by("lastName", "firstName")
            .ascending());
        assertThat(persons).hasSize(3).containsExactly(donny, dave, oliver).isEqualTo(persons2);

        Page<Person> personsPage = repository.findTop3ByLastNameStartingWith("M",
            PageRequest.of(0, 3, Sort.by("lastName", "firstName").ascending()));
        assertThat(personsPage.get()).containsExactly(donny, dave, oliver);
    }

    @Test
    void findByNestedStringSimplePropertyStartingWith() {
        stefan.setFriend(oliver);
        repository.save(stefan);
        carter.setFriend(dave);
        repository.save(carter);

        List<Person> result = repository.findByFriendFirstNameStartsWith("D");
        assertThat(result)
            .hasSize(1)
            .containsExactly(carter);

        TestUtils.setFriendsToNull(repository, stefan, carter);
    }

    @Test
    void findBySimplePropertyStartingWith_String_LimitedWithOffset() {
        Page<Person> first = repository.findByLastNameStartsWithOrderByAgeAsc("Mo", PageRequest.of(0, 1));

        assertThat(first.getNumberOfElements()).isEqualTo(1);
        assertThat(first.getTotalPages()).isEqualTo(2);
        assertThat(first.get()).hasSize(1).containsOnly(leroi2);
        assertThat(first.get()).hasSize(1).containsOnly(leroi2);

        Page<Person> last = repository.findByLastNameStartsWithOrderByAgeAsc("Mo", first.nextPageable());

        assertThat(last.getTotalPages()).isEqualTo(2);
        assertThat(last.getNumberOfElements()).isEqualTo(1);
        assertThat(last.get()).hasSize(1).containsAnyOf(leroi);

        Page<Person> all = repository.findByLastNameStartsWithOrderByAgeAsc("Mo", PageRequest.of(0, 5));

        assertThat(all.getTotalPages()).isEqualTo(1);
        assertThat(all.getNumberOfElements()).isEqualTo(2);
        assertThat(all.get()).hasSize(2).containsOnly(leroi, leroi2);
    }

    @Test
    void findDistinctByNestedSimpleProperty_NegativeTest() {
        oliver.setFriend(alicia);
        repository.save(oliver);
        dave.setFriend(leroi);
        repository.save(dave);
        carter.setFriend(leroi2);
        repository.save(carter);

        assertThatThrownBy(() -> repository.findDistinctByFriendFirstNameStartsWith("l"))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage("DISTINCT queries are currently supported only for the first level objects, got a query for " +
                "friend.firstName");

        TestUtils.setFriendsToNull(repository, oliver, dave, carter);
    }
}
