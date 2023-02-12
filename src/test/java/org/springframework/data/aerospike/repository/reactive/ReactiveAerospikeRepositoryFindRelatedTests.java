package org.springframework.data.aerospike.repository.reactive;

import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.CustomerSomeFields;
import org.springframework.data.aerospike.sample.ReactiveCustomerRepository;
import org.springframework.data.domain.Sort;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.domain.Sort.Order.asc;

/**
 * @author Igor Ermolenko
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReactiveAerospikeRepositoryFindRelatedTests extends BaseReactiveIntegrationTests {

    @Autowired
    ReactiveCustomerRepository customerRepo;

    private Customer customer1, customer2, customer3, customer4;

    @BeforeAll
    public void setUpBeforeAll() {
        customerRepo.deleteAll().block();

        customer1 = Customer.builder().id(nextId()).firstName("Homer").lastName("Simpson").age(42).group('a').build();
        customer2 = Customer.builder().id(nextId()).firstName("Marge").lastName("Simpson").age(39).group('b').build();
        customer3 = Customer.builder().id(nextId()).firstName("Bart").lastName("Simpson").age(15).group('b').build();
        customer4 = Customer.builder().id(nextId()).firstName("Matt").lastName("Groening").age(65).group('c').build();

        additionalAerospikeTestOperations.createIndexIfNotExists(Customer.class, "customer_first_name_index",
            "firstname", IndexType.STRING);
        additionalAerospikeTestOperations.createIndexIfNotExists(Customer.class, "customer_last_name_index",
            "lastname", IndexType.STRING);
        additionalAerospikeTestOperations.createIndexIfNotExists(Customer.class, "customer_age_index", "age",
            IndexType.NUMERIC);

        customerRepo.saveAll(Flux.just(customer1, customer2, customer3, customer4))
            .subscribeOn(Schedulers.parallel()).collectList().block();
    }

    @Test
    public void findById_ShouldReturnExistent() {
        Customer result = customerRepo.findById(customer2.getId())
            .subscribeOn(Schedulers.parallel()).block();

        assertThat(result).isEqualTo(customer2);
    }

    @Test
    public void findById_ShouldNotReturnNotExistent() {
        Customer result = customerRepo.findById("non-existent-id")
            .subscribeOn(Schedulers.parallel()).block();

        assertThat(result).isNull();
    }

    @Test
    public void findByIdPublisher_ShouldReturnFirst() {
        Publisher<String> ids = Flux.just(customer2.getId(), customer4.getId());

        Customer result = customerRepo.findById(ids).subscribeOn(Schedulers.parallel()).block();
        assertThat(result).isEqualTo(customer2);
    }

    @Test
    public void findByIdPublisher_NotReturnFirstNotExistent() {
        Publisher<String> ids = Flux.just("non-existent-id", customer2.getId(), customer4.getId());

        Customer result = customerRepo.findById(ids).subscribeOn(Schedulers.parallel()).block();
        assertThat(result).isNull();
    }

    @Test
    public void findAll_ShouldReturnAll() {
        List<Customer> results = customerRepo.findAll().subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(results)
            .containsOnly(customer1, customer2, customer3, customer4);
    }

    @Test
    public void findAllByIDsIterable_ShouldReturnAllExistent() {
        Iterable<String> ids = asList(customer2.getId(), "non-existent-id", customer4.getId());

        List<Customer> results = customerRepo.findAllById(ids)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(customer2, customer4);
    }

    @Test
    public void findAllByIDsPublisher_ShouldReturnAllExistent() {
        Publisher<String> ids = Flux.just(customer1.getId(), customer2.getId(), customer4.getId(), "non-existent-id");

        List<Customer> results = customerRepo.findAllById(ids)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(customer1, customer2, customer4);
    }

    @Test
    public void findByLastname_ShouldWorkProperly() {
        List<Customer> results = customerRepo.findByLastName("Simpson")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(customer1, customer2, customer3);
    }

    @Test
    public void findCustomerSomeFieldsByLastname_ShouldWorkProperlyProjection() {
        List<CustomerSomeFields> results = customerRepo.findCustomerSomeFieldsByLastName("Simpson")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(customer1.toCustomerSomeFields(), customer2.toCustomerSomeFields(),
            customer3.toCustomerSomeFields());
    }

    @Test
    public void findDynamicTypeByLastname_ShouldWorkProperlyDynamicProjection() {
        List<CustomerSomeFields> results = customerRepo.findByLastName("Simpson", CustomerSomeFields.class)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(customer1.toCustomerSomeFields(), customer2.toCustomerSomeFields(),
            customer3.toCustomerSomeFields());
    }

    @Test
    public void findByLastnameName_ShouldWorkProperly() {
        List<Customer> results = customerRepo.findByLastNameNot("Simpson")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(customer4);
    }

    @Test
    public void findOneByLastname_ShouldWorkProperly() {
        Customer result = customerRepo.findOneByLastName("Groening")
            .subscribeOn(Schedulers.parallel()).block();

        assertThat(result).isEqualTo(customer4);
    }

    @Test
    public void findByLastnameOrderByFirstnameAsc_ShouldWorkProperly() {
        List<Customer> results = customerRepo.findByLastNameOrderByFirstNameAsc("Simpson")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactly(customer3, customer1, customer2);
    }

    @Test
    public void findByLastnameOrderByFirstnameDesc_ShouldWorkProperly() {
        List<Customer> results = customerRepo.findByLastNameOrderByFirstNameDesc("Simpson")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactly(customer2, customer1, customer3);
    }

    @Test
    public void findByFirstnameEndsWith_ShouldWorkProperly() {
        List<Customer> results = customerRepo.findByFirstNameEndsWith("t")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(customer3, customer4);
    }

    @Test
    public void findByFirstnameStartsWithOrderByAgeAsc_ShouldWorkProperly() {
        List<Customer> results = customerRepo.findByFirstNameStartsWithOrderByAgeAsc("Ma")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactly(customer2, customer4);
    }

    @Test
    public void findCustomerSomeFieldsByFirstnameStartsWithOrderByAgeAsc_ShouldWorkProperly() {
        List<CustomerSomeFields> results =
            customerRepo.findCustomerSomeFieldsByFirstNameStartsWithOrderByFirstNameAsc("Ma")
                .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactly(customer2.toCustomerSomeFields(), customer4.toCustomerSomeFields());
    }

    @Test
    public void findByAgeLessThan_ShouldWorkProperly() {
        List<Customer> results = customerRepo.findByAgeLessThan(40, Sort.by(asc("firstName")))
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactly(customer3, customer2);
    }

    @Test
    public void findByFirstnameIn_ShouldWorkProperly() {
        List<Customer> results = customerRepo.findByFirstNameIn(asList("Matt", "Homer"))
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(customer1, customer4);
    }

    @Test
    public void findByFirstnameAndLastname_ShouldWorkProperly() {
        List<Customer> results = customerRepo.findByFirstNameAndLastName("Bart", "Simpson")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(customer3);
    }

    @Test
    public void findOneByFirstnameAndLastname_ShouldWorkProperly() {
        Customer result = customerRepo.findByFirstNameAndLastName("Bart", "Simpson")
            .subscribeOn(Schedulers.parallel()).blockLast();

        assertThat(result).isEqualTo(customer3);
    }

    @Test
    public void findByLastnameAndAge_ShouldWorkProperly() {
        Customer result = customerRepo.findByLastNameAndAge("Simpson", 15)
            .subscribeOn(Schedulers.parallel()).blockLast();

        assertThat(result).isEqualTo(customer3);
    }

    @Test
    public void findByAgeBetween_ShouldWorkProperly() {
        List<Customer> results = customerRepo.findByAgeBetween(10, 40)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(customer2, customer3);
    }

    @Test
    public void findByFirstnameContains_ShouldWorkProperly() {
        List<Customer> results = customerRepo.findByFirstNameContains("ar")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(customer2, customer3);
    }

    @Test
    public void findByFirstnameContainingIgnoreCase_ShouldWorkProperly() {
        List<Customer> results = customerRepo.findByFirstNameContainingIgnoreCase("m")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(customer1, customer2, customer4);
    }

    @Test
    public void findByAgeBetweenAndLastname_ShouldWorkProperly() {
        List<Customer> results = customerRepo.findByAgeBetweenAndLastName(30, 70, "Simpson")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(customer1, customer2);
    }

    @Test
    public void findByAgeBetweenOrderByFirstnameDesc_ShouldWorkProperly() {
        List<Customer> results = customerRepo.findByAgeBetweenOrderByFirstNameDesc(30, 70)
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsExactly(customer4, customer2, customer1);
    }

    @Test
    public void findByGroup() {
        List<Customer> results = customerRepo.findByGroup('b')
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).containsOnly(customer2, customer3);
    }
}
