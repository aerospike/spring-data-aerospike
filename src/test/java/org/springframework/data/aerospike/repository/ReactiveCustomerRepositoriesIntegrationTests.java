package org.springframework.data.aerospike.repository;


import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.ReactiveCustomerRepository;


/**
 * @author Igor Ermolenko
 */
public class ReactiveCustomerRepositoriesIntegrationTests extends BaseIntegrationTests {

    @Autowired
    ReactiveCustomerRepository repository;

    @Test
    public void testCreate() {
        Customer customer = new Customer("dave-001", "Dave", "Matthews");
        repository.save(customer).block();
    }
}
