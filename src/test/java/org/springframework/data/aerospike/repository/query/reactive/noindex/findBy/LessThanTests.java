package org.springframework.data.aerospike.repository.query.reactive.noindex.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.reactive.noindex.ReactiveCustomerRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.domain.Sort;
import reactor.core.scheduler.Schedulers;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.domain.Sort.Order.asc;

/**
 * Tests for the "Is less than" reactive repository query. Keywords: LessThan, IsLessThan.
 */
public class LessThanTests extends ReactiveCustomerRepositoryQueryTests {

    @Test
    public void findByAgeLessThan_ShouldWorkProperly() {
        List<Customer> results = reactiveRepository.findByAgeLessThan(40, Sort.by(asc("firstName")))
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactly(bart, leela, lisa, maggie, marge);
    }
}
