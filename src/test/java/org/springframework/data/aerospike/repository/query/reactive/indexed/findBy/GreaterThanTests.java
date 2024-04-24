package org.springframework.data.aerospike.repository.query.reactive.indexed.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.repository.query.reactive.indexed.ReactiveIndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import reactor.core.scheduler.Schedulers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the "Is greater than" repository query. Keywords: GreaterThan, IsGreaterThan.
 */
public class GreaterThanTests extends ReactiveIndexedPersonRepositoryQueryTests {

    @Test
    @AssertBinsAreIndexed(binNames = "age", entityClass = IndexedPerson.class)
    public void findBySimplePropertyGreaterThan_Integer_Paginated() {
        assertStmtHasSecIndexFilter("findByAgeGreaterThan", IndexedPerson.class, 1, PageRequest.of(0, 1));
        Page<IndexedPerson> page = reactiveRepository.findByAgeGreaterThan(1, PageRequest.of(0, 1))
            .subscribeOn(Schedulers.parallel()).block();
        assertThat(page).containsAnyElementsOf(allIndexedPersons);

        Slice<IndexedPerson> slice = reactiveRepository.findByAgeGreaterThan(1, PageRequest.of(0, 2))
            .subscribeOn(Schedulers.parallel()).block();
        assertThat(slice).hasSize(2).containsAnyElementsOf(allIndexedPersons);

        Slice<IndexedPerson> sliceSorted = reactiveRepository.findByAgeGreaterThan(1, PageRequest.of(1, 2, Sort.by(
                "age")))
            .subscribeOn(Schedulers.parallel()).block();
        assertThat(sliceSorted).hasSize(2).containsAnyElementsOf(allIndexedPersons);

        assertThatThrownBy(() -> reactiveRepository.findByAgeGreaterThan(1, PageRequest.of(1, 2)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unsorted query must not have offset value. For retrieving paged results use sorted query.");
    }

    @Test
    @AssertBinsAreIndexed(binNames = "age", entityClass = IndexedPerson.class)
    public void findBySimplePropertyGreaterThan_Integer_Unpaged() {
        assertStmtHasSecIndexFilter("findByAgeGreaterThan", IndexedPerson.class, 40, Pageable.unpaged());
        Slice<IndexedPerson> slice = reactiveRepository.findByAgeGreaterThan(40, Pageable.unpaged())
            .subscribeOn(Schedulers.parallel()).block();
        assertThat(slice.hasContent()).isTrue();
        assertThat(slice.getNumberOfElements()).isGreaterThan(0);
        assertThat(slice.hasNext()).isFalse();
        assertThat(slice.isLast()).isTrue();
    }
}
