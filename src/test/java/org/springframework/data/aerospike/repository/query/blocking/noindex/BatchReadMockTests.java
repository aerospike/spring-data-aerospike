package org.springframework.data.aerospike.repository.query.blocking.noindex;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.BatchPolicy;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.ArgumentMatchers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.PersonRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for verifying the amount of chunks used in batch read
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BatchReadMockTests extends BaseBlockingIntegrationTests {

    private final int CHUNKS_AMOUNT = 3;
    @MockitoSpyBean
    private IAerospikeClient trackedClient;
    @Autowired
    private PersonRepository<Person> repository;
    private List<String> ids;

    @BeforeAll
    void beforeAll() {
        assertThat(template.getAerospikeConverter().getAerospikeDataSettings().getBatchReadSize())
            .isEqualTo(100);
        ids = IntStream.range(0, 201)
            .mapToObj(count -> "as-" + count)
            .toList();
        Record[] records = ids.stream()
            .map(id -> new Record(Map.of(id, id), 1, 1))
            .toArray(Record[]::new);

        // By default, spy will call the real methods
        // Simulate the results and use doReturn().when() to treat cases when arguments (like 'records') might be null
        // during Mockito's internal processing
        doReturn(records)
            .when(trackedClient)
            .get(any(BatchPolicy.class), ArgumentMatchers.<Key[]>any());
        doReturn(records)
            .when(trackedClient)
            .get(any(BatchPolicy.class), ArgumentMatchers.<Key[]>any(), ArgumentMatchers.<String[]>any());
    }

    @Test
    void findAllById() {
        repository.findAllById(ids);
        verify(trackedClient, times(CHUNKS_AMOUNT)).get(any(BatchPolicy.class), ArgumentMatchers.<Key[]>any());
    }

    @Test
    void findAllById_withSorting() {
        repository.findAllById(ids, Sort.by("firstName"));
        verify(trackedClient, times(CHUNKS_AMOUNT)).get(any(BatchPolicy.class), ArgumentMatchers.<Key[]>any());
    }

    @Test
    void findAllById_withPagination() {
        repository.findAllById(ids, PageRequest.of(1, 1, Sort.by("firstName")));
        verify(trackedClient, times(CHUNKS_AMOUNT)).get(any(BatchPolicy.class), ArgumentMatchers.<Key[]>any());
    }

    @Test
    void findAllById_andSimpleProperty() {
        repository.findAllByIdAndFirstName(QueryParam.of(ids), QueryParam.of("David"));
        verify(trackedClient, times(CHUNKS_AMOUNT)).get(any(BatchPolicy.class), ArgumentMatchers.<Key[]>any());
    }
}
