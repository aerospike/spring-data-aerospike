package org.springframework.data.aerospike.repository.query.reactive.noindex.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.query.reactive.noindex.ReactiveCustomerRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Customer;
import reactor.core.scheduler.Schedulers;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.query.QueryParam.of;

/**
 * Tests for the "Is between" reactive repository query. Keywords: Between, IsBetween.
 */
public class BetweenTests extends ReactiveCustomerRepositoryQueryTests {

    @Test
    public void findBySimplePropertyBetween() {
        List<Customer> results = reactiveRepository.findByAgeBetween(10, 40)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(marge, bart, leela);
    }

    @Test
    public void findBySimplePropertyBetween_AND_SimpleProperty() {
        QueryParam ageBetween = of(30, 70);
        QueryParam lastName = of("Simpson");
        List<Customer> results = reactiveRepository.findByAgeBetweenAndLastName(ageBetween, lastName)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(homer, marge);
    }

    @Test
    public void findBySimplePropertyBetween_OrderByFirstnameDesc() {
        List<Customer> results = reactiveRepository.findByAgeBetweenOrderByFirstNameDesc(30, 70)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactly(matt, marge, homer);
    }
}
