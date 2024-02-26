package org.springframework.data.aerospike.repository.query.findBy.noindex;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.utility.TestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is like" repository query. Keywords: Like, IsLike.
 */
public class IsLikeTests extends PersonRepositoryQueryTests {

    @Test
    void findBySimplePropertyLike_String() {
        List<Person> persons = repository.findByFirstNameLike("Ca.*er");
        assertThat(persons).contains(carter);

        List<Person> persons0 = repository.findByFirstNameLikeIgnoreCase("CART.*er");
        assertThat(persons0).contains(carter);

        List<Person> persons1 = repository.findByFirstNameLike(".*ve.*");
        assertThat(persons1).contains(dave, oliver);

        List<Person> persons2 = repository.findByFirstNameLike("Carr.*er");
        assertThat(persons2).isEmpty();
    }

    @Test
    void findByNestedStringSimplePropertyLike() {
        oliver.setFriend(dave);
        repository.save(oliver);
        carter.setFriend(stefan);
        repository.save(carter);

        List<Person> result = repository.findByFriendLastNameLike(".*tthe.*");
        assertThat(result).contains(oliver);

        TestUtils.setFriendsToNull(repository, oliver, carter);
    }

    @Test
    void findByExactMapKeyWithValueLike() {
        assertThat(donny.getStringMap()).containsKey("key1");
        assertThat(donny.getStringMap()).containsValue("val1");
        assertThat(boyd.getStringMap()).containsKey("key1");
        assertThat(boyd.getStringMap()).containsValue("val1");

        List<Person> persons = repository.findByStringMapLike("key1", "^.*al1$");
        assertThat(persons).contains(donny, boyd);
    }
}
