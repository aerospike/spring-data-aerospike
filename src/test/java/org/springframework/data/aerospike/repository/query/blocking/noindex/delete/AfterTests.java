package org.springframework.data.aerospike.repository.query.blocking.delete;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.noindex.PersonRepositoryQueryTests;

import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is after" repository query. Keywords: After, IsAfter.
 */
public class AfterTests extends PersonRepositoryQueryTests {

    @Test
    void deleteByDateSimplePropertyAfter() {
        dave.setDateOfBirth(new Date());
        repository.save(dave);

        Date date = new Date(126230400);
        assertThat(repository.findByDateOfBirthAfter(date)).isNotEmpty();

        repository.deleteByDateOfBirthAfter(date);

        assertThat(repository.findByDateOfBirthAfter(date)).isEmpty();

        dave.setDateOfBirth(null);
        repository.save(dave);
    }

    @Test
    void deleteByDateSimplePropertyAfter_NoMatchingRecords() {
        Date date = new Date();
        assertThat(repository.findByDateOfBirthAfter(date)).isEmpty();

        repository.deleteByDateOfBirthAfter(date);

        assertThat(repository.findByDateOfBirthAfter(date)).isEmpty();
    }
}
