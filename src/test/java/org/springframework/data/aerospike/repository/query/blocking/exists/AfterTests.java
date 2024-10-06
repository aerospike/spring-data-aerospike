package org.springframework.data.aerospike.repository.query.blocking.exists;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.PersonRepositoryQueryTests;

import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is after" repository query. Keywords: After, IsAfter.
 */
public class AfterTests extends PersonRepositoryQueryTests {

    @Test
    void existsByDateSimplePropertyAfter() {
        dave.setDateOfBirth(new Date());
        repository.save(dave);

        boolean result = repository.existsByDateOfBirthAfter(new Date(126230400));
        assertThat(result).isTrue();

        dave.setDateOfBirth(null);
        repository.save(dave);
    }

    @Test
    void existsByDateSimplePropertyAfter_NoMatchingRecords() {
        boolean result = repository.existsByDateOfBirthAfter(new Date());
        assertThat(result).isFalse();
    }
}
