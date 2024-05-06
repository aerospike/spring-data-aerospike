package org.springframework.data.aerospike.repository.query.reactive.indexed.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.repository.query.reactive.indexed.ReactiveIndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import reactor.core.scheduler.Schedulers;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is less than" repository query. Keywords: LessThan, IsLessThan.
 */
public class LessThanTests extends ReactiveIndexedPersonRepositoryQueryTests {

    @Test
    @AssertBinsAreIndexed(binNames = "age", entityClass = IndexedPerson.class)
    public void findBySimplePropertyLessThan_Integer_Unpaged() {
        assertQueryHasSecIndexFilter("findByAgeLessThan", IndexedPerson.class, 40, Pageable.unpaged());
        Page<IndexedPerson> page = reactiveRepository.findByAgeLessThan(40, Pageable.unpaged())
            .subscribeOn(Schedulers.parallel()).block();
        assertThat(page.hasContent()).isTrue();
        assertThat(page.getNumberOfElements()).isGreaterThan(1);
        assertThat(page.hasNext()).isFalse();
        assertThat(page.getTotalPages()).isEqualTo(1);
        assertThat(page.getTotalElements()).isEqualTo(page.getSize());
    }
}
