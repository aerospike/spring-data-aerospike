package org.springframework.data.aerospike.repository.query.reactive.indexed.find;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.annotation.Extensive;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.repository.query.reactive.indexed.ReactiveIndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.List;

import static com.aerospike.client.query.IndexType.NUMERIC;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is between" repository query. Keywords: Between, IsBetween.
 */
@Extensive
public class ReactiveIndexedFindBetweenTests extends ReactiveIndexedPersonRepositoryQueryTests {

    @Override
    protected List<Index> newIndexes() {
        List<Index> newIndexes = new ArrayList<>();
        newIndexes.add(Index.builder()
                .set(reactiveTemplate.getSetName(IndexedPerson.class))
                .name("indexed_person_age_" + "r_find_between")
                .bin("age")
                .indexType(NUMERIC)
                .build());
        return newIndexes;
    }

    @Test
    @Disabled
    @AssertBinsAreIndexed(binNames = "age", entityClass = IndexedPerson.class)
    public void findBySimplePropertyBetween_Integer() {
        assertQueryHasSecIndexFilter("findByAgeBetween", IndexedPerson.class, 39, 45);
        List<IndexedPerson> results = reactiveRepository.findByAgeBetween(39, 45)
                .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(results).hasSize(2).contains(alain, luc);
    }
}
