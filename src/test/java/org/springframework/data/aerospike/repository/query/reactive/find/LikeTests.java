package org.springframework.data.aerospike.repository.query.reactive.find;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.query.reactive.ReactiveCustomerRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Customer;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.query.QueryParam.of;

/**
 * Tests for the "Is like" repository query. Keywords: Like, IsLike.
 */
public class LikeTests extends ReactiveCustomerRepositoryQueryTests {

    @Test
    void findBySimplePropertyLike_String() {
        List<Customer> results = reactiveRepository.findByFirstNameLike("Mat.*")
            .collectList().block();
        assertThat(results).containsOnly(matt);
    }

    @Test
    void findByIdLike_String() {
        List<Customer> results = reactiveRepository.findByIdLike("as-.*")
            .collectList().block();
        assertThat(results).contains(matt);

        QueryParam idLike = of("as-.*");
        QueryParam name = of(marge.getFirstName());
        results = reactiveRepository.findByIdLikeAndFirstName(idLike, name)
            .collectList().block();
        assertThat(results).contains(marge);

        QueryParam ids = of(List.of(marge.getId(), homer.getId()));
        results = reactiveRepository.findByIdLikeAndId(idLike, ids)
            .collectList().block();
        assertThat(results).contains(marge, homer);
    }
}
