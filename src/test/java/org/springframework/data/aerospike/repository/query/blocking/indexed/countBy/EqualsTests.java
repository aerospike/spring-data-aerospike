package org.springframework.data.aerospike.repository.query.blocking.indexed.countBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.indexed.IndexedPersonRepositoryQueryTests;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the "Equals" repository query. Keywords: Is, Equals (or no keyword).
 */
public class EqualsTests extends IndexedPersonRepositoryQueryTests {

    @Test
    public void countBySimpleProperty_String_NegativeTest() {
        assertThatThrownBy(() -> repository.countByLastName("Lerois"))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Query method IndexedPerson.countByLastName is not supported");

        assertThatThrownBy(() -> repository.countByLastName("Smirnova"))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Query method IndexedPerson.countByLastName is not supported");
    }
}
