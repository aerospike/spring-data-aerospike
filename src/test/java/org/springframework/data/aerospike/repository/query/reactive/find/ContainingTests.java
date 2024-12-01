package org.springframework.data.aerospike.repository.query.reactive.find;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.reactive.ReactiveCustomerRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Customer;
import reactor.core.scheduler.Schedulers;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Contains" reactive repository query. Keywords: Containing, IsContaining, Contains.
 */
public class ContainingTests extends ReactiveCustomerRepositoryQueryTests {

    @Test
    public void findBySimplePropertyContaining() {
        List<Customer> results = reactiveRepository.findByFirstNameContains("ar")
            .collectList().block();

        assertThat(results).containsOnly(marge, bart);
    }

    @Test
    public void findBySimplePropertyContaining_IgnoreCase() {
        List<Customer> results = reactiveRepository.findByFirstNameContainingIgnoreCase("m")
            .collectList().block();

        assertThat(results).containsOnly(homer, marge, matt, maggie);
    }
}
