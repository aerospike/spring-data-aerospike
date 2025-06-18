package org.springframework.data.aerospike.repository.query.reactive;

import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.ArgumentMatchers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.PersonRepository;
import org.springframework.data.aerospike.sample.ReactiveCustomerRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Tests for verifying the amount of chunks used in batch write using real interaction with database
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BatchWriteChunksTests extends BaseReactiveIntegrationTests {

    private final int CHUNKS_AMOUNT = 3;
    @MockitoSpyBean
    private IAerospikeReactorClient trackedClient;
    @Autowired
    private ReactiveCustomerRepository reactiveRepository;
    private List<String> ids;
    private List<Customer> customers;

    @BeforeAll
    void beforeAll() {
        assertThat(reactiveTemplate.getAerospikeConverter().getAerospikeDataSettings().getBatchReadSize())
            .isEqualTo(100);
        ids = IntStream.range(0, 201)
            .mapToObj(count -> "as-" + count)
            .toList();
        customers = ids.stream()
            .map(id -> (Customer) Customer.builder()
                .id(id)
                .build())
            .toList();
    }

    @AfterEach
    void afterEach() {
        reactiveRepository.deleteAllById(ids).block();
    }

    @Test
    void saveAll() {
        reactiveRepository.saveAll(customers).collectList().block();
        verify(trackedClient, times(CHUNKS_AMOUNT)).operate(any(BatchPolicy.class), any());
    }

    @Test
    void insertAll() {
        reactiveTemplate.insertAll(customers).collectList().block();
        verify(trackedClient, times(CHUNKS_AMOUNT)).operate(any(BatchPolicy.class), any());
    }

    @Test
    void updateAll() {
        reactiveTemplate.insertAll(customers).collectList().block();
        var personsUpdated = customers.stream()
            .peek(customer -> customer.setFirstName("John"))
            .toList();
        reactiveTemplate.updateAll(personsUpdated).collectList().block();
        // Both insertAll and updateAll call the same client.operate() method
        verify(trackedClient, times(CHUNKS_AMOUNT * 2)).operate(any(BatchPolicy.class), any());
    }

    @Test
    void deleteAll() {
        reactiveTemplate.insertAll(customers).collectList().block();
        reactiveRepository.deleteAll(customers).block();
        // Both insertAll and updateAll call the same client.operate() method
        verify(trackedClient, times(CHUNKS_AMOUNT * 2)).operate(any(BatchPolicy.class), any());
    }

    @Test
    void deleteAllById() {
        reactiveTemplate.insertAll(customers).collectList().block();
        reactiveRepository.deleteAllById(ids).block();
        // Both insertAll and updateAll call the same client.operate() method
        verify(trackedClient, times(CHUNKS_AMOUNT)).delete(any(BatchPolicy.class), any(), any());
    }
}
