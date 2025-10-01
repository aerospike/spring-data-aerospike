package org.springframework.data.aerospike.repository.query.reactive.noindex;

import com.aerospike.client.BatchRead;
import com.aerospike.client.Key;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.ArgumentMatchers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.sample.ReactiveCustomerRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import reactor.core.publisher.Mono;

import java.util.List;
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
public class BatchReadMockTests extends BaseReactiveIntegrationTests {

    private final int CHUNKS_AMOUNT = 3;
    @MockitoSpyBean
    private IAerospikeReactorClient trackedClient;
    @Autowired
    private ReactiveCustomerRepository reactiveRepository;
    private List<String> ids;

    @BeforeAll
    void beforeAll() {
        assertThat(reactiveTemplate.getAerospikeConverter().getAerospikeDataSettings().getBatchReadSize())
            .isEqualTo(100);
        ids = IntStream.range(0, 201)
            .mapToObj(count -> "as-" + count)
            .toList();
        List<BatchRead> batchReads = ids.stream()
            .map(id -> new BatchRead(new Key(namespace, "TEST", id), new String[]{id}))
            .toList();

        // By default, spy will call the real methods
        // Simulate the results and use doReturn().when() to treat cases when arguments (like 'records') might be null
        // during Mockito's internal processing
        doReturn(Mono.just(batchReads))
            .when(trackedClient)
            .get(any(BatchPolicy.class), ArgumentMatchers.<List<BatchRead>>any());
    }

    @Test
    void findAllById() {
        reactiveRepository.findAllById(ids).collectList().block();
        verify(trackedClient, times(CHUNKS_AMOUNT)).get(any(BatchPolicy.class), ArgumentMatchers.<List<BatchRead>>any());
    }

    @Test
    void findAllById_withSorting() {
        reactiveRepository.findAllById(ids, Sort.by("firstName")).collectList().block();
        verify(trackedClient, times(CHUNKS_AMOUNT)).get(any(BatchPolicy.class), ArgumentMatchers.<List<BatchRead>>any());
    }

    @Test
    void findAllById_withPagination() {
        reactiveRepository.findAllById(ids, PageRequest.of(1, 1, Sort.by("firstName")))
            .block();
        verify(trackedClient, times(CHUNKS_AMOUNT)).get(any(BatchPolicy.class), ArgumentMatchers.<List<BatchRead>>any());
    }

    @Test
    void findAllById_andSimpleProperty() {
        reactiveRepository.findAllByIdAndFirstName(QueryParam.of(ids), QueryParam.of("David")).block();
        verify(trackedClient, times(CHUNKS_AMOUNT)).get(any(BatchPolicy.class), ArgumentMatchers.<List<BatchRead>>any());
    }
}
