package org.springframework.data.aerospike.repository.query.blocking.delete;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.noindex.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is greater than" repository query. Keywords: GreaterThan, IsGreaterThan.
 */
public class GreaterThanTests extends PersonRepositoryQueryTests {

    @Test
    void deleteBySimpleProperty_Integer_Paginated() {
        List<Person> findQueryResults = repository.findByAgeGreaterThan(40);
        assertThat(findQueryResults).isNotEmpty();

        repository.deleteByAgeGreaterThan(40, PageRequest.of(0, 1));
        List<Person> findQueryResultsAfterDelete = repository.findByAgeGreaterThan(40);
        assertThat(findQueryResultsAfterDelete.size()).isNotZero().isLessThan(findQueryResults.size());

        // Query to delete results from a non-existing page, no records must be deleted
        repository.deleteByAgeGreaterThan(40, PageRequest.of(10, 1, Sort.by("firstName")));
        List<Person> findQueryResultsAfterDelete2 = repository.findByAgeGreaterThan(40);
        assertThat(findQueryResultsAfterDelete2).containsExactlyInAnyOrderElementsOf(findQueryResultsAfterDelete);
    }
}
