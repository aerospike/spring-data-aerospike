package org.springframework.data.aerospike.repository.query.blocking.indexed.find;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.annotation.Nightly;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.query.model.IndexedField;
import org.springframework.data.aerospike.repository.query.blocking.indexed.IndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.aerospike.client.query.IndexType.NUMERIC;
import static com.aerospike.client.query.IndexType.STRING;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is greater than" repository query. Keywords: GreaterThan, IsGreaterThan.
 */
@Nightly
public class BlockingIndexedFindGreaterThanTests extends IndexedPersonRepositoryQueryTests {

    @Override
    protected List<Index> newIndexes() {
        List<Index> newIndexes = new ArrayList<>();
        String setName = template.getSetName(IndexedPerson.class);
        String postfix = "find_gt";
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_first_name_" + postfix)
                .bin("firstName")
                .indexType(STRING)
                .build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_age_" + postfix)
                .bin("age")
                .indexType(NUMERIC)
                .build());
        return newIndexes;
    }

    @Test
    @AssertBinsAreIndexed(binNames = "firstName", entityClass = IndexedPerson.class)
    public void findBySimplePropertyGreaterThan_String_NoSecondaryIndexFilter() {
        // "Greater than a String" has no secondary index Filter
        assertThat(queryHasSecIndexFilter("findByFirstNameGreaterThan", IndexedPerson.class, "Bill")).isFalse();
        List<IndexedPerson> result = repository.findByFirstNameGreaterThan("Bill");
        assertThat(result).containsAll(allIndexedPersons);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "age", entityClass = IndexedPerson.class)
    public void findBySimplePropertyGreaterThan_Integer_Paginated() {
        assertThat(indexesCache.hasIndexFor(
                new IndexedField(getNameSpace(), template.getSetName(IndexedPerson.class), "age"))
        ).withFailMessage(String.format("No index for 'age' bin in %s.personSetNameGtTests", getNameSpace())).isTrue();
        Slice<IndexedPerson> slice = repository.findByAgeGreaterThan(40, PageRequest.of(0, 10));
        assertThat(slice.hasContent()).isTrue();
        assertThat(slice.hasNext()).isFalse();
        assertThat(slice.getContent()).hasSize(3).contains(john, jane, peter);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "age", entityClass = IndexedPerson.class)
    public void findBySimplePropertyGreaterThan_Integer_Paginated_respectsLimitAndOffsetAndSort() {
        assertQueryHasSecIndexFilter("findByAgeGreaterThan", IndexedPerson.class, 40);
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
        assertQueryHasSecIndexFilter("findByAgeGreaterThan", IndexedPerson.class, 40);
        Slice<IndexedPerson> first = repository.findByAgeGreaterThan(40, PageRequest.of(0, 1, Sort.by("age")));
        assertThat(first.hasContent()).isTrue();
        assertThat(first.getNumberOfElements()).isEqualTo(1);
        assertThat(first.hasNext()).isTrue();
        assertThat(first.isFirst()).isTrue();
        assertThat(first.isLast()).isFalse();

        assertQueryHasSecIndexFilter("findByAgeGreaterThan", IndexedPerson.class, 40);
        Slice<IndexedPerson> last = repository.findByAgeGreaterThan(40, PageRequest.of(2, 1, Sort.by("age")));
        assertThat(last.hasContent()).isTrue();
        assertThat(last.getNumberOfElements()).isEqualTo(1);
        assertThat(last.hasNext()).isFalse();
        assertThat(last.isLast()).isTrue();

        assertQueryHasSecIndexFilter("findByAgeGreaterThan", IndexedPerson.class, 100);
        Slice<IndexedPerson> slice = repository.findByAgeGreaterThan(100, PageRequest.of(0, 10));
        assertThat(slice.hasContent()).isFalse();
        assertThat(slice.hasNext()).isFalse();
        assertThat(slice.getContent()).isEmpty();
    }
}
