package org.springframework.data.aerospike.repository.query.reactive.indexed.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.query.reactive.indexed.ReactiveIndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;
import reactor.core.scheduler.Schedulers;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Equals" repository query. Keywords: Is, Equals (or no keyword).
 */
public class EqualsTests extends ReactiveIndexedPersonRepositoryQueryTests {

    @Test
    public void findBySimpleProperty_String() {
        List<IndexedPerson> results = reactiveRepository.findByLastName("Coutant-Kerbalec")
            .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(results).containsOnly(petra, emilien);

        List<IndexedPerson> results2 = reactiveRepository.findByFirstName("Lilly")
            .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(results2).containsExactlyInAnyOrder(lilly);
    }

    @Test
    public void findBySimpleProperty_String_AND_SimpleProperty_Integer() {
        QueryParam firstName = QueryParam.of("Lilly");
        QueryParam age = QueryParam.of(28);
        List<IndexedPerson> results = reactiveRepository.findByFirstNameAndAge(firstName, age)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(lilly);
    }

    @Test
    public void findByNestedSimpleProperty_String() {
        String zipCode = "C0123";
        assertThat(alain.getAddress().getZipCode()).isEqualTo(zipCode);

        List<IndexedPerson> results = reactiveRepository.findByAddressZipCode(zipCode)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).contains(alain);
    }
}
