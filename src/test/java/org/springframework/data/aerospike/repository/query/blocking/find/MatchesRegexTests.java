package org.springframework.data.aerospike.repository.query.blocking.find;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.util.TestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Matches regex" repository query. Keywords: Regex, MatchesRegex, Matches.
 */
public class MatchesRegexTests extends PersonRepositoryQueryTests {

    @Test
    void findBySimplePropertyMatchingRegex_String() {
        List<Person> persons = repository.findByFirstNameMatchesRegex("Ca.*er");
        assertThat(persons).contains(carter);

        persons = repository.findByFirstNameMatches("Ca.*er");
        assertThat(persons).contains(carter);

        persons = repository.findByFirstNameRegex("Ca.*er");
        assertThat(persons).contains(carter);

        List<Person> persons0 = repository.findByFirstNameMatchesRegexIgnoreCase("CART.*er");
        assertThat(persons0).contains(carter);

        List<Person> persons1 = repository.findByFirstNameMatchesRegex(".*ve.*");
        assertThat(persons1).contains(dave, oliver);

        List<Person> persons2 = repository.findByFirstNameMatchesRegex("Carr.*er");
        assertThat(persons2).isEmpty();
    }

    @Test
    void findByNestedStringSimplePropertyMatchingRegex() {
        oliver.setFriend(dave);
        repository.save(oliver);
        carter.setFriend(stefan);
        repository.save(carter);

        List<Person> result = repository.findByFriendLastNameLike(".*tthe.*");
        assertThat(result).contains(oliver);

        result = repository.findByFriendLastNameMatchesRegex(".*tthe.*");
        assertThat(result).contains(oliver);
        TestUtils.setFriendsToNull(repository, oliver, carter);
    }
}
