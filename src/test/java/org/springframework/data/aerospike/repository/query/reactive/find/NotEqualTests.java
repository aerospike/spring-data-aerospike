package org.springframework.data.aerospike.repository.query.reactive.find;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.reactive.ReactiveCustomerRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Customer;
import reactor.core.scheduler.Schedulers;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is not equal" reactive repository query. Keywords: Not, IsNot.
 */
public class NotEqualTests extends ReactiveCustomerRepositoryQueryTests {

    @Test
    public void findBySimplePropertyNot() {
        List<Customer> results = reactiveRepository.findByLastNameNot("Simpson")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).contains(matt);
    }
}
