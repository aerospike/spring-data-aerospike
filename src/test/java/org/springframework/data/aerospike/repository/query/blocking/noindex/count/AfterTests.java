package org.springframework.data.aerospike.repository.query.blocking.noindex.count;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.noindex.PersonRepositoryQueryTests;

import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is after" repository query. Keywords: After, IsAfter.
 */
public class AfterTests extends PersonRepositoryQueryTests {

    @Test
    void countByDateSimplePropertyAfter() {
        dave.setDateOfBirth(new Date());
        repository.save(dave);

        long result = repository.countByDateOfBirthAfter(new Date(126230400));
        assertThat(result).isEqualTo(1);

        dave.setDateOfBirth(null);
        repository.save(dave);
    }

    @Test
    void countByDateSimplePropertyAfter_NoMatchingRecords() {
        long result = repository.countByDateOfBirthAfter(new Date());
        assertThat(result).isZero();
    }
}
