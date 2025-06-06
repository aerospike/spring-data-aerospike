package org.springframework.data.aerospike.repository.query.blocking.find;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.query.blocking.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.util.TestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.query.QueryParam.of;

/**
 * Tests for the "Is like" repository query. Keywords: Like, IsLike.
 */
public class LikeTests extends PersonRepositoryQueryTests {

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
    // "findByIdLike" uses filter expression (scan operation), only for String ids
    void findByIdLike_String() {
        List<Person> persons = repository.findByIdLike("as-.*");
        assertThat(persons).containsExactlyInAnyOrderElementsOf(allPersons);

        QueryParam idLike = of("as-.*");
        QueryParam name = of(carter.getFirstName());
        persons = repository.findByIdLikeAndFirstName(idLike, name);
        assertThat(persons).containsOnly(carter);

        QueryParam ids = of(List.of(carter.getId(), dave.getId()));
        persons = repository.findByIdLikeAndId(idLike, ids);
        assertThat(persons).containsOnly(carter, dave);
    }

    @Test
    void findByNestedSimplePropertyLike_String() {
        oliver.setFriend(dave);
        repository.save(oliver);
        carter.setFriend(stefan);
        repository.save(carter);

        List<Person> result = repository.findByFriendLastNameLike(".*tthe.*");
        assertThat(result).contains(oliver);

        TestUtils.setFriendsToNull(repository, oliver, carter);
    }
}
