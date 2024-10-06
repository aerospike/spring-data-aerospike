package org.springframework.data.aerospike.repository.query.reactive.find;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.reactive.ReactiveCustomerRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.CustomerSomeFields;
import reactor.core.scheduler.Schedulers;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Starts with" reactive repository query. Keywords: StartingWith, IsStartingWith, StartsWith.
 */
public class StartsWithTests extends ReactiveCustomerRepositoryQueryTests {

    @Test
    public void findByFirstnameStartsWithOrderByAgeAsc_ShouldWorkProperly() {
        List<Customer> results = reactiveRepository.findByFirstNameStartsWithOrderByAgeAsc("Ma")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactly(maggie, marge, matt);
    }

    @Test
    public void findCustomerSomeFieldsByFirstnameStartsWithOrderByAgeAsc_ShouldWorkProperly() {
        List<CustomerSomeFields> results =
            reactiveRepository.findCustomerSomeFieldsByFirstNameStartsWithOrderByFirstNameAsc("Ma")
                .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactly( maggie.toCustomerSomeFields(), marge.toCustomerSomeFields(),
            matt.toCustomerSomeFields());
    }
}
