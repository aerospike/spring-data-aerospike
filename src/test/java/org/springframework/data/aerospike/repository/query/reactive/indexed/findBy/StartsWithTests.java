package org.springframework.data.aerospike.repository.query.reactive.indexed.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.config.NoSecondaryIndexRequired;
import org.springframework.data.aerospike.repository.query.reactive.indexed.ReactiveIndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.aerospike.util.TestUtils;
import reactor.core.scheduler.Schedulers;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the "Starts with" repository query. Keywords: StartingWith, IsStartingWith, StartsWith.
 */
public class StartsWithTests extends ReactiveIndexedPersonRepositoryQueryTests {

    @Test
    @AssertBinsAreIndexed(binNames = "lastName", entityClass = IndexedPerson.class)
    void findBySimplePropertyStartingWith_String_Distinct() {
        assertStmtHasSecIndexFilter("findDistinctByLastNameStartingWith", IndexedPerson.class, "Coutant-Kerbalec");
        List<IndexedPerson> persons = reactiveRepository.findDistinctByLastNameStartingWith("Coutant-Kerbalec")
            .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(persons).hasSize(1);

        assertStmtHasSecIndexFilter("findByLastNameStartingWith", IndexedPerson.class, "Coutant-Kerbalec");
        List<IndexedPerson> persons2 = reactiveRepository.findByLastNameStartingWith("Coutant-Kerbalec")
            .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(persons2).hasSize(2);
    }

    @Test
    @NoSecondaryIndexRequired
    void findByNestedSimplePropertyStartingWith_String_Distinct_NegativeTest() {
        alain.setFriend(luc);
        reactiveRepository.save(alain);
        lilly.setFriend(petra);
        reactiveRepository.save(lilly);
        daniel.setFriend(emilien);
        reactiveRepository.save(daniel);

        assertThatThrownBy(() -> reactiveRepository.findDistinctByFriendLastNameStartingWith("l"))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage("DISTINCT queries are currently supported only for the first level objects, got a query for " +
                "friend.lastName");

        TestUtils.setFriendsToNull(reactiveRepository, alain, lilly, daniel);
    }
}
