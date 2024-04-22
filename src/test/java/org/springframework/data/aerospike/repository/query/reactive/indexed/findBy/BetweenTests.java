package org.springframework.data.aerospike.repository.query.reactive.indexed.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.repository.query.reactive.indexed.ReactiveIndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;
import reactor.core.scheduler.Schedulers;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is between" repository query. Keywords: Between, IsBetween.
 */
public class BetweenTests extends ReactiveIndexedPersonRepositoryQueryTests {

    @Test
    @AssertBinsAreIndexed(binNames = "age", entityClass = IndexedPerson.class)
    public void findBySimplePropertyBetween_Integer() {
        assertStmtHasSecIndexFilter("findByAgeBetween", IndexedPerson.class, 39, 45);
        List<IndexedPerson> results = reactiveRepository.findByAgeBetween(39, 45)
            .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(results).hasSize(2).contains(alain, luc);
    }
}
