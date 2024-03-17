package org.springframework.data.aerospike.repository.query.blocking.indexed.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.indexed.IndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is less than" repository query. Keywords: LessThan, IsLessThan.
 */
public class LessThanTests extends IndexedPersonRepositoryQueryTests {

    @Test
    public void findBySimplePropertyLessThan_String() {
        List<IndexedPerson> result = repository.findByFirstNameGreaterThan("Bill");
        assertThat(result).containsAll(allIndexedPersons);
    }
}
