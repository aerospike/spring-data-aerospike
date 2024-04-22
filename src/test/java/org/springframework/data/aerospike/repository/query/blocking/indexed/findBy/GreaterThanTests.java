package org.springframework.data.aerospike.repository.query.blocking.indexed.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.repository.query.blocking.indexed.IndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is greater than" repository query. Keywords: GreaterThan, IsGreaterThan.
 */
public class GreaterThanTests extends IndexedPersonRepositoryQueryTests {

    @Test
    @AssertBinsAreIndexed(binNames = "firstName", entityClass = IndexedPerson.class)
    public void findBySimplePropertyGreaterThan_String_NoSecondaryIndexFilter() {
        // "Greater than a String" has no secondary index Filter
        assertThat(stmtHasSecIndexFilter("findByFirstNameGreaterThan", IndexedPerson.class, "Bill")).isFalse();
        List<IndexedPerson> result = repository.findByFirstNameGreaterThan("Bill");
        assertThat(result).containsAll(allIndexedPersons);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "age", entityClass = IndexedPerson.class)
    public void findBySimplePropertyGreaterThan_Integer_Paginated() {
        assertStmtHasSecIndexFilter("findByAgeGreaterThan", IndexedPerson.class, 40);
        Slice<IndexedPerson> slice = repository.findByAgeGreaterThan(40, PageRequest.of(0, 10));
        assertThat(slice.hasContent()).isTrue();
        assertThat(slice.hasNext()).isFalse();
        assertThat(slice.getContent()).hasSize(3).contains(john, jane, peter);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "age", entityClass = IndexedPerson.class)
    public void findBySimplePropertyGreaterThan_Integer_Paginated_respectsLimitAndOffsetAndSort() {
        assertStmtHasSecIndexFilter("findByAgeGreaterThan", IndexedPerson.class, 40);
        List<IndexedPerson> result = IntStream.range(0, 4)
            .mapToObj(index -> repository.findByAgeGreaterThan(40, PageRequest.of(index, 1, Sort.by("age"))))
            .flatMap(slice -> slice.getContent().stream())
            .collect(Collectors.toList());

        assertThat(result)
            .hasSize(3)
            .containsSequence(peter, john, jane);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "age", entityClass = IndexedPerson.class)
    public void findBySimplePropertyGreaterThan_Integer_Paginated_validHasPrevAndHasNext() {
        assertStmtHasSecIndexFilter("findByAgeGreaterThan", IndexedPerson.class, 40);
        Slice<IndexedPerson> first = repository.findByAgeGreaterThan(40, PageRequest.of(0, 1, Sort.by("age")));
        assertThat(first.hasContent()).isTrue();
        assertThat(first.getNumberOfElements()).isEqualTo(1);
        assertThat(first.hasNext()).isTrue();
        assertThat(first.isFirst()).isTrue();
        assertThat(first.isLast()).isFalse();

        assertStmtHasSecIndexFilter("findByAgeGreaterThan", IndexedPerson.class, 40);
        Slice<IndexedPerson> last = repository.findByAgeGreaterThan(40, PageRequest.of(2, 1, Sort.by("age")));
        assertThat(last.hasContent()).isTrue();
        assertThat(last.getNumberOfElements()).isEqualTo(1);
        assertThat(last.hasNext()).isFalse();
        assertThat(last.isLast()).isTrue();

        assertStmtHasSecIndexFilter("findByAgeGreaterThan", IndexedPerson.class, 100);
        Slice<IndexedPerson> slice = repository.findByAgeGreaterThan(100, PageRequest.of(0, 10));
        assertThat(slice.hasContent()).isFalse();
        assertThat(slice.hasNext()).isFalse();
        assertThat(slice.getContent()).isEmpty();
    }
}
