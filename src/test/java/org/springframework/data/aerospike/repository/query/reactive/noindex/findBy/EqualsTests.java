package org.springframework.data.aerospike.repository.query.reactive.noindex.findBy;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.query.reactive.noindex.ReactiveCustomerRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.CustomerSomeFields;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.query.QueryParam.of;

/**
 * Tests for the "Equals" reactive repository query. Keywords: Is, Equals (or no keyword).
 */
public class EqualsTests extends ReactiveCustomerRepositoryQueryTests {

    @Test
    public void findById_ShouldReturnExistent() {
        Customer result = reactiveRepository.findById(marge.getId())
            .subscribeOn(Schedulers.parallel()).block();

        assertThat(result).isEqualTo(marge);
    }

    @Test
    public void findById_ShouldNotReturnNonExistent() {
        Customer result = reactiveRepository.findById("non-existent-id")
            .subscribeOn(Schedulers.parallel()).block();

        assertThat(result).isNull();
    }

    @Test
    public void findByIdPublisher_ShouldReturnFirst() {
        Publisher<String> ids = Flux.just(marge.getId(), matt.getId());

        Customer result = reactiveRepository.findById(ids).subscribeOn(Schedulers.parallel()).block();
        assertThat(result).isEqualTo(marge);
    }

    @Test
    public void findByIdPublisher_ShouldNotReturnFirstNonExistent() {
        Publisher<String> ids = Flux.just("non-existent-id", marge.getId(), matt.getId());

        Customer result = reactiveRepository.findById(ids).subscribeOn(Schedulers.parallel()).block();
        assertThat(result).isNull();
    }

    @Test
    public void findAll_ShouldReturnAll() {
        List<Customer> results = reactiveRepository.findAll().subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(results).contains(homer, marge, bart, matt);
    }

    @Test
    public void findAllByIdsIterable_ShouldReturnAllExistent() {
        Iterable<String> ids = asList(marge.getId(), "non-existent-id", matt.getId());

        List<Customer> results = reactiveRepository.findAllById(ids)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(marge, matt);
    }

    @Test
    public void findAllByIDsPublisher_ShouldReturnAllExistent() {
        Publisher<String> ids = Flux.just(homer.getId(), marge.getId(), matt.getId(), "non-existent-id");

        List<Customer> results = reactiveRepository.findAllById(ids)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(homer, marge, matt);
    }

    @Test
    public void findBySimpleProperty() {
        List<Customer> results = reactiveRepository.findByLastName("Simpson")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(homer, marge, bart, lisa, maggie);
    }

    @Test
    public void findBySimpleProperty_Projection() {
        List<CustomerSomeFields> results = reactiveRepository.findCustomerSomeFieldsByLastName("Simpson")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).contains(homer.toCustomerSomeFields(), marge.toCustomerSomeFields(),
            bart.toCustomerSomeFields(), lisa.toCustomerSomeFields(), maggie.toCustomerSomeFields());
    }

    @Test
    public void findDynamicTypeBySimpleProperty_DynamicProjection() {
        List<CustomerSomeFields> results = reactiveRepository
            .findByLastName("Simpson", CustomerSomeFields.class)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(homer.toCustomerSomeFields(), marge.toCustomerSomeFields(),
            bart.toCustomerSomeFields(), lisa.toCustomerSomeFields(), maggie.toCustomerSomeFields());
    }

    @Test
    public void findOneBySimpleProperty() {
        Customer result = reactiveRepository.findOneByLastName("Groening")
            .subscribeOn(Schedulers.parallel()).block();

        assertThat(result).isEqualTo(matt);
    }

    @Test
    public void findBySimpleProperty_OrderByAsc() {
        List<Customer> results = reactiveRepository.findByLastNameOrderByFirstNameAsc("Simpson")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).contains(bart, homer, marge);
    }

    @Test
    public void findBySimpleProperty_OrderByDesc() {
        List<Customer> results = reactiveRepository.findByLastNameOrderByFirstNameDesc("Simpson")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).contains(marge, homer, bart);
    }

    @Test
    public void findBySimpleProperty_AND_SimpleProperty_String() {
        QueryParam firstName = of("Bart");
        QueryParam lastName = of("Simpson");
        Customer result = reactiveRepository.findByFirstNameAndLastName(firstName, lastName)
            .subscribeOn(Schedulers.parallel()).blockLast();

        assertThat(result).isEqualTo(bart);
    }

    @Test
    public void findBySimpleProperty_AND_SimpleProperty_Integer() {
        QueryParam lastName = of("Simpson");
        QueryParam age = of(10);
        Customer result = reactiveRepository.findByLastNameAndAge(lastName, age)
            .subscribeOn(Schedulers.parallel()).blockLast();

        assertThat(result).isEqualTo(bart);
    }

    @Test
    public void findBySimpleProperty_Char() {
        List<Customer> results = reactiveRepository.findByGroup('b')
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(marge, bart);
    }
}
