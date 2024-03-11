package org.springframework.data.aerospike.repository.query.findBy.noindex;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.utility.TestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is less than or equal" repository query. Keywords: LessThanEqual, IsLessThanEqual.
 */
public class LessThanOrEqualTests extends PersonRepositoryQueryTests {

    @Test
    void findByNestedSimplePropertyLessThanOrEqual_Integer() {
        alicia.setFriend(boyd);
        repository.save(alicia);
        dave.setFriend(oliver);
        repository.save(dave);
        carter.setFriend(dave);
        repository.save(carter);
        leroi.setFriend(carter);
        repository.save(leroi);

        List<Person> result = repository.findByFriendAgeLessThanEqual(42);

        assertThat(result)
            .hasSize(2)
            .containsExactlyInAnyOrder(dave, carter);

        TestUtils.setFriendsToNull(repository, alicia, dave, carter, leroi);
    }
}
