package org.springframework.data.aerospike.repository.query.blocking;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.BatchPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.PersonRepository;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for verifying the amount of chunks used in batch write using real interaction with database
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BatchWriteChunksTests extends BaseBlockingIntegrationTests {

    private final int CHUNKS_AMOUNT = 3;
    @MockitoSpyBean
    private IAerospikeClient trackedClient;
    @Autowired
    private PersonRepository<Person> repository;
    private List<String> ids;
    private List<Person> persons;

    @BeforeAll
    void beforeAll() {
        assertThat(converter.getAerospikeDataSettings().getBatchReadSize()).isEqualTo(100);
        ids = IntStream.range(0, 201)
            .mapToObj(count -> "as-" + count)
            .toList();
        persons = ids.stream()
            .map(id -> (Person) Person.builder()
                .id(id)
                .build())
            .toList();
    }

    @AfterEach
    void afterEach() {
        repository.deleteAllById(ids);
    }

    @Test
    void saveAll() {
        repository.saveAll(persons);
        verify(trackedClient, times(CHUNKS_AMOUNT)).operate(any(BatchPolicy.class), any());
    }

    @Test
    void insertAll() {
        template.insertAll(persons);
        verify(trackedClient, times(CHUNKS_AMOUNT)).operate(any(BatchPolicy.class), any());
    }

    @Test
    void updateAll() {
        template.insertAll(persons);
        var personsUpdated = persons.stream()
            .peek(person -> person.setFirstName("John"))
            .toList();
        template.updateAll(personsUpdated);
        // Both insertAll and updateAll call the same client.operate() method
        verify(trackedClient, times(CHUNKS_AMOUNT * 2)).operate(any(BatchPolicy.class), any());
    }

    @Test
    void deleteAll() {
        template.insertAll(persons);
        repository.deleteAll(persons);
        // Both insertAll and updateAll call the same client.operate() method
        verify(trackedClient, times(CHUNKS_AMOUNT * 2)).operate(any(BatchPolicy.class), any());
    }

    @Test
    void deleteAllById() {
        template.insertAll(persons);
        repository.deleteAllById(ids);
        // Both insertAll and updateAll call the same client.operate() method
        verify(trackedClient, times(CHUNKS_AMOUNT)).delete(any(BatchPolicy.class), any(), any());
    }
}
