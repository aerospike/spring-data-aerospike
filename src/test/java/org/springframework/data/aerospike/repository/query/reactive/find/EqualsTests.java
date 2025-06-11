package org.springframework.data.aerospike.repository.query.reactive.find;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.query.reactive.ReactiveCustomerRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.CustomerSomeFields;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.testcontainers.shaded.com.google.common.collect.Streams;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.query.QueryParam.of;

/**
 * Tests for the "Equals" reactive repository query. Keywords: Is, Equals (or no keyword).
 */
public class EqualsTests extends ReactiveCustomerRepositoryQueryTests {

    @Test
    public void findById_ShouldReturnExistent() {
        Customer result = reactiveRepository.findById(marge.getId()).block();

        assertThat(result).isEqualTo(marge);
    }

    @Test
    public void findById_ShouldNotReturnNonExistent() {
        Customer result = reactiveRepository.findById("non-existent-id").block();

        assertThat(result).isNull();
    }

    @Test
    public void findByIdPublisher_ShouldReturnFirst() {
        Publisher<String> ids = Flux.just(marge.getId(), matt.getId());

        Customer result = reactiveRepository.findById(ids).block();
        assertThat(result).isEqualTo(marge);
    }

    @Test
    public void findByIdPublisher_ShouldNotReturnFirstNonExistent() {
        Publisher<String> ids = Flux.just("non-existent-id", marge.getId(), matt.getId());

        Customer result = reactiveRepository.findById(ids).block();
        assertThat(result).isNull();
    }

    @Test
    public void findAll_ShouldReturnAll() {
        List<Customer> results = reactiveRepository.findAll().collectList().block();
        assertThat(results).contains(homer, marge, bart, matt);
    }

    @Test
    public void findAllByIdsIterable_ShouldReturnAllExistent() {
        Iterable<String> idsIncludingNonExistent = asList(marge.getId(), "non-existent-id", matt.getId());
        reactiveRepository.findAllById(idsIncludingNonExistent)
            .collectList()
            .as(StepVerifier::create)
            .assertNext(results -> {
                assertThat(results).containsOnly(marge, matt);
            })
            .verifyComplete();

        Iterable<String> idsNonExistent = asList("1", "non-existent-id", "2");
        reactiveRepository.findAllById(idsNonExistent)
            .collectList()
            .as(StepVerifier::create)
            .assertNext(results -> {
                assertThat(results).isEmpty();
            })
            .verifyComplete();
    }

    @Test
    public void findAllByIDsPublisher_ShouldReturnAllExistent() {
        Publisher<String> ids = Flux.just(homer.getId(), marge.getId(), matt.getId(), "non-existent-id");
        reactiveRepository.findAllById(ids)
            .collectList()
            .as(StepVerifier::create)
            .assertNext(results -> {
                assertThat(results).containsOnly(homer, marge, matt);
            })
            .verifyComplete();
    }

    @Test
    void findAllByIds_paginatedQuery() {
        List<String> ids = allCustomers.stream().map(Customer::getId).toList();

        reactiveRepository.findAllById(ids, Pageable.ofSize(8))
            .as(StepVerifier::create)
            .assertNext(result -> {
                assertThat(result.getTotalPages()).isEqualTo(1);
                assertThat(result).hasSameElementsAs(allCustomers);
            })
            .verifyComplete();

        reactiveRepository.findAllById(ids, PageRequest.ofSize(7))
            .as(StepVerifier::create)
            .assertNext(result -> {
                assertThat(result.getTotalPages()).isEqualTo(2);
            })
            .verifyComplete();

        List<String> firstNamesSorted = allCustomers.stream().map(Customer::getFirstName).sorted().toList();
        assertThat(firstNamesSorted.indexOf(leela.getFirstName())).isEqualTo(2);
        assertThat(firstNamesSorted.indexOf(lisa.getFirstName())).isEqualTo(3);
        reactiveRepository.findAllById(ids, PageRequest.of(1, 2, Sort.by("firstName")))
            .as(StepVerifier::create)
            .assertNext(result -> {
                assertThat(result.getTotalPages()).isEqualTo(4);
                Iterator<Customer> iterator = result.iterator();
                assertThat(iterator.next()).isEqualTo(leela);
                assertThat(iterator.next()).isEqualTo(lisa);
                assertThat(iterator.hasNext()).isFalse();
            })
            .verifyComplete();

        reactiveRepository.findAllById(ids, Pageable.unpaged())
            .as(StepVerifier::create)
            .assertNext(result -> {
                assertThat(result.getTotalPages()).isEqualTo(1);
                assertThat(result.getTotalElements()).isEqualTo(8);
            })
            .verifyComplete();
    }

    @Test
    void findAllByIds_paginatedQuery_withSorting_shouldReturnAllExisting() {
        var idsIncludingNonExistent = Streams.concat(
            allCustomers.stream().map(Customer::getId),
            Stream.of("1", "2") // Non-existent ids
        ).toList();
        reactiveRepository.findAllById(idsIncludingNonExistent, Pageable.ofSize(8))
            .as(StepVerifier::create)
            .assertNext(result -> {
                // The necessary slice is chosen after retrieving all results
                // That's why the total page count is 1 here (two non-existent ids, no results retrieved for them)
                assertThat(result.getTotalPages()).isEqualTo(1);
                assertThat(result).hasSameElementsAs(allCustomers);
            })
            .verifyComplete();

        reactiveRepository.findAllById(idsIncludingNonExistent, PageRequest.ofSize(7))
            .as(StepVerifier::create)
            .assertNext(result -> {
                // The necessary slice is chosen after retrieving all results
                // That's why the total page count is 2 here (two non-existent ids, no results retrieved for them)
                assertThat(result.getTotalPages()).isEqualTo(2);
            })
            .verifyComplete();

        List<String> firstNamesSorted = allCustomers.stream().map(Customer::getFirstName).sorted().toList();
        assertThat(firstNamesSorted.indexOf(leela.getFirstName())).isEqualTo(2);
        assertThat(firstNamesSorted.indexOf(lisa.getFirstName())).isEqualTo(3);
        reactiveRepository.findAllById(idsIncludingNonExistent, PageRequest.of(1, 2, Sort.by("firstName")))
            .as(StepVerifier::create)
            .assertNext(result -> {
                // The necessary slice is chosen after retrieving all results
                // That's why the total page count is 4 here (two non-existent ids, no results retrieved for them)
                assertThat(result.getTotalPages()).isEqualTo(4);
                Iterator<Customer> iterator = result.iterator();
                assertThat(iterator.next()).isEqualTo(leela);
                assertThat(iterator.next()).isEqualTo(lisa);
                assertThat(iterator.hasNext()).isFalse();
            })
            .verifyComplete();

        var idsNonExisting = List.of("1", "2");
        reactiveRepository.findAllById(idsNonExisting, PageRequest.of(1, 2, Sort.by("firstName")))
            .as(StepVerifier::create)
            .assertNext(result -> {
                // No existing records with the given ids
                assertThat(result.getTotalPages()).isEqualTo(0);
                assertThat(result.getTotalElements()).isEqualTo(0);
            })
            .verifyComplete();
    }

    @Test
    void findAllByIds_paginatedQuery_withOffset_originalOrder_unsorted() {
        List<String> ids = allCustomers.stream().map(Customer::getId).toList();
        assertThat(ids.size()).isEqualTo(8);
        assertThat(ids.indexOf(bart.getId())).isEqualTo(2);
        assertThat(ids.indexOf(lisa.getId())).isEqualTo(3);

        // Paginated queries with offset and no sorting (i.e. original order in ids collection)
        // are only allowed for purely id queries
        reactiveRepository.findAllById(ids, PageRequest.of(1, 2))
            .as(StepVerifier::create)
            .assertNext(result -> {
                assertThat(result.getTotalPages()).isEqualTo(4); // Overall ids quantity is 8
                Iterator<Customer> iterator = result.iterator();
                assertThat(iterator.next()).isEqualTo(bart);
                assertThat(iterator.next()).isEqualTo(lisa);
                assertThat(iterator.hasNext()).isFalse();
            })
            .verifyComplete();
    }

    @Test
    void findAllByIds_paginatedQuery_unsorted_shouldReturnAllExisting() {
        List<String> ids = allCustomers.stream().map(Customer::getId).toList();
        assertThat(ids.size()).isEqualTo(8);
        assertThat(ids.indexOf(bart.getId())).isEqualTo(2);
        assertThat(ids.indexOf(lisa.getId())).isEqualTo(3);
        var idsIncludingNonExistent = Streams.concat(ids.stream(), Stream.of("1", "2")).toList();

        // Paginated queries with offset and no sorting (i.e. original order in ids collection)
        // are only allowed for purely id queries
        reactiveRepository.findAllById(idsIncludingNonExistent, PageRequest.of(1, 2))
            .as(StepVerifier::create)
            .assertNext(result -> {
                // The necessary slice is chosen after retrieving all results
                // That's why the total page count is 4 here (two non-existent ids, no results retrieved for them)
                assertThat(result.getTotalPages()).isEqualTo(4); // Overall existing ids quantity is 8
                Iterator<Customer> iterator = result.iterator();
                assertThat(iterator.next()).isEqualTo(bart);
                assertThat(iterator.next()).isEqualTo(lisa);
                assertThat(iterator.hasNext()).isFalse();
            })
            .verifyComplete();

        var idsNonExistent = List.of("1", "2");
        // Paginated queries with offset and no sorting (i.e. original order in ids collection)
        // are only allowed for purely id queries
        reactiveRepository.findAllById(idsNonExistent, PageRequest.of(1, 2))
            .as(StepVerifier::create)
            .assertNext(result -> {
                // The necessary slice is chosen after retrieving all results
                // That's why the total page count is 0 here (two non-existent ids, no results retrieved for them)
                assertThat(result.getTotalPages()).isEqualTo(0);
                Iterator<Customer> iterator = result.iterator();
                assertThat(iterator.hasNext()).isFalse();
            })
            .verifyComplete();
    }

    @Test
    void findAllByIds_sorted() {
        List<String> ids = allCustomers.stream().map(Customer::getId).toList();

        reactiveRepository.findAllById(ids, Sort.by(Sort.Direction.DESC, "firstName")).collectList()
            .as(StepVerifier::create)
            .assertNext(result -> {
                assertThat(result).hasSameElementsAs(allCustomers);
                assertThat(result.iterator().next()).isEqualTo(fry);
            })
            .verifyComplete();

        reactiveRepository.findAllById(ids, Sort.by(Sort.Direction.ASC, "firstName")).collectList()
            .as(StepVerifier::create)
            .assertNext(result -> {
                assertThat(result).hasSameElementsAs(allCustomers);
                assertThat(result.iterator().next()).isEqualTo(bart);
            })
            .verifyComplete();
    }

    @Test
    public void findBySimpleProperty() {
        List<Customer> results = reactiveRepository.findByLastName("Simpson")
            .collectList().block();

        assertThat(results).containsOnly(homer, marge, bart, lisa, maggie);
    }

    @Test
    public void findBySimpleProperty_Projection() {
        List<CustomerSomeFields> results = reactiveRepository.findCustomerSomeFieldsByLastName("Simpson")
            .collectList().block();

        assertThat(results).contains(homer.toCustomerSomeFields(), marge.toCustomerSomeFields(),
            bart.toCustomerSomeFields(), lisa.toCustomerSomeFields(), maggie.toCustomerSomeFields());
    }

    @Test
    public void findDynamicTypeBySimpleProperty_DynamicProjection() {
        List<CustomerSomeFields> results = reactiveRepository
            .findByLastName("Simpson", CustomerSomeFields.class)
            .collectList().block();

        assertThat(results).containsOnly(homer.toCustomerSomeFields(), marge.toCustomerSomeFields(),
            bart.toCustomerSomeFields(), lisa.toCustomerSomeFields(), maggie.toCustomerSomeFields());
    }

    @Test
    public void findOneBySimpleProperty() {
        Customer result = reactiveRepository.findOneByLastName("Groening").block();

        assertThat(result).isEqualTo(matt);
    }

    @Test
    public void findBySimpleProperty_OrderByAsc() {
        List<Customer> results = reactiveRepository.findByLastNameOrderByFirstNameAsc("Simpson")
            .collectList().block();

        assertThat(results).contains(bart, homer, marge);
    }

    @Test
    public void findBySimpleProperty_OrderByDesc() {
        List<Customer> results = reactiveRepository.findByLastNameOrderByFirstNameDesc("Simpson")
            .collectList().block();

        assertThat(results).contains(marge, homer, bart);
    }

    @Test
    public void findBySimpleProperty_AND_SimpleProperty_String() {
        QueryParam firstName = of("Bart");
        QueryParam lastName = of("Simpson");
        Customer result = reactiveRepository.findByFirstNameAndLastName(firstName, lastName)
            .blockLast();

        assertThat(result).isEqualTo(bart);
    }

    @Test
    public void findBySimpleProperty_AND_SimpleProperty_Integer() {
        QueryParam lastName = of("Simpson");
        QueryParam age = of(10);
        Customer result = reactiveRepository.findByLastNameAndAge(lastName, age)
            .blockLast();

        assertThat(result).isEqualTo(bart);
    }

    @Test
    public void findBySimpleProperty_Char() {
        List<Customer> results = reactiveRepository.findByGroup('b')
            .collectList().block();

        assertThat(results).containsOnly(marge, bart);
    }

    @Test
    void findAllByIds_AND_simpleProperty_paginated() {
        QueryParam ids = of(List.of(maggie.getId(), matt.getId()));
        QueryParam names = of(List.of(maggie.getFirstName(), matt.getFirstName()));

        reactiveRepository.findAllByIdAndFirstNameIn(ids, names, Pageable.ofSize(1))
            .as(StepVerifier::create)
            .assertNext(result -> {
                assertThat(result.getSize()).isEqualTo(1);
                assertThat(result.getContent()).containsAnyOf(maggie, matt);
                assertThat(result.hasNext()).isTrue();
            })
            .verifyComplete();

        reactiveRepository.findAllByIdAndFirstNameIn(ids, names, Pageable.unpaged())
            .as(StepVerifier::create)
            .assertNext(result -> {
                assertThat(result.getSize()).isEqualTo(2);
                assertThat(result.getContent()).containsExactlyInAnyOrder(maggie, matt);
                assertThat(result.hasNext()).isFalse();
            })
            .verifyComplete();

        reactiveRepository.findAllByIdAndFirstNameIn(ids, names, PageRequest.of(1, 1, Sort.by("firstName")))
            .as(StepVerifier::create)
            .assertNext(result -> {
                assertThat(result.getSize()).isEqualTo(1);
                assertThat(result.getContent()).containsOnly(matt); // it is the second result out of the given two
                assertThat(result.hasNext()).isFalse();
            })
            .verifyComplete();

        QueryParam idsAll = of(allCustomers.stream().map(Customer::getId).toList());
        QueryParam namesAll = of(allCustomers.stream().map(Customer::getFirstName).toList());
        reactiveRepository.findAllByIdAndFirstNameIn(idsAll, namesAll, PageRequest.of(1, 1, Sort.by("firstName")))
            .as(StepVerifier::create)
            .assertNext(result -> {
                assertThat(result.getSize()).isEqualTo(1);
                assertThat(result.getContent()).containsOnly(homer);
                assertThat(result.hasNext()).isTrue();
            })
            .verifyComplete();
    }

    @Test
    void findAllByIds_AND_simpleProperty_paginated_shouldReturnAllExisting() {
        QueryParam ids = of(List.of("1", "2", maggie.getId(), matt.getId()));
        QueryParam names = of(List.of(maggie.getFirstName(), matt.getFirstName(), "testName"));
//
//        reactiveRepository.findAllByIdAndFirstNameIn(ids, names, Pageable.ofSize(1))
//            .as(StepVerifier::create)
//            .assertNext(result -> {
//                assertThat(result.getSize()).isEqualTo(1);
//                assertThat(result.getContent()).containsAnyOf(maggie, matt);
//                assertThat(result.hasNext()).isTrue();
//            })
//            .verifyComplete();
//
//        reactiveRepository.findAllByIdAndFirstNameIn(ids, names, Pageable.unpaged())
//            .as(StepVerifier::create)
//            .assertNext(result -> {
//                assertThat(result.getSize()).isEqualTo(2);
//                assertThat(result.getContent()).containsExactlyInAnyOrder(maggie, matt);
//                assertThat(result.hasNext()).isFalse();
//            })
//            .verifyComplete();
//
//        reactiveRepository.findAllByIdAndFirstNameIn(ids, names,
//                PageRequest.of(1, 1, Sort.by("firstName"))
//            )
//            .as(StepVerifier::create)
//            .assertNext(result -> {
//                assertThat(result.getSize()).isEqualTo(1);
//                assertThat(result.getContent()).containsOnly(matt); // it is the second existing record
//                assertThat(result.hasNext()).isFalse();
//            })
//            .verifyComplete();

        QueryParam idsNonExistent = of(List.of("1", "2"));
        reactiveRepository.findAllByIdAndFirstNameIn(idsNonExistent, names,
                PageRequest.of(1, 1, Sort.by("firstName"))
            )
            .as(StepVerifier::create)
            .assertNext(result -> {
                // No existing records, size being 1 is a result of Slice's getSize() returning page size in this case
                assertThat(result.getSize()).isEqualTo(1);
                assertThat(result.getContent()).isEmpty();
                assertThat(result.hasNext()).isFalse();
            })
            .verifyComplete();
    }

    @Test
    void findAllByIds_AND_simpleProperty_sorted() {
        QueryParam ids = of(List.of(fry.getId(), leela.getId(), matt.getId()));
        QueryParam names = of(List.of(fry.getFirstName(), leela.getFirstName(), matt.getFirstName()));
        reactiveRepository.findAllByIdAndFirstNameIn(ids, names, Sort.by(Sort.Direction.DESC, "firstName")).collectList()
            .as(StepVerifier::create)
            .assertNext(result -> {
                assertThat(result.get(0)).isEqualTo(fry);
            })
            .verifyComplete();

        reactiveRepository.findAllByIdAndFirstNameIn(ids, names, Sort.by(Sort.Direction.ASC, "firstName")).collectList()
            .as(StepVerifier::create)
            .assertNext(result -> {
                assertThat(result.get(0)).isEqualTo(leela);
            })
            .verifyComplete();
    }
}
