/*******************************************************************************
 * Copyright (c) 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 * 		https://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.springframework.data.aerospike.repository;


import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.CustomerRepository;

import java.util.Arrays;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * @author Oliver Gierke
 */
public class CustomerRepositoriesIntegrationTests extends BaseIntegrationTests {

	@Autowired CustomerRepository repository;

	@Test
	public void testCreate() {
		repository.save(Customer.builder().id("dave-001").firstname("Dave").lastname("Matthews").build());
	}

	@Test
	public void testExists() {
		repository.save(Customer.builder().id("dave-001").firstname("Dave").lastname("Matthews").build());

		boolean exists = repository.existsById("dave-001");

		assertThat(exists).isTrue();
	}

	@Test
	public void testDelete() {
		repository.delete(Customer.builder().id("dave-001").firstname("Dave").lastname("Matthews").build());
	}

	@Test
	public void testReadById() {
		Customer customer = repository.save(Customer.builder().id("dave-001").firstname("Dave").lastname("Matthews").build());

		Optional<Customer> findById = repository.findById("dave-001");

		assertThat(findById).hasValueSatisfying(actual -> {
			assertThat(actual.getLastname()).isEqualTo(customer.getLastname());
			assertThat(actual.getFirstname()).isEqualTo(customer.getFirstname());
		});
	}

	@Test
	public void testFindAllByIDs(){
		repository.save(Customer.builder().id("dave-001").firstname("Dave").lastname("AMatthews").build());
		repository.save(Customer.builder().id("dave-002").firstname("Dave").lastname("BMatthews").build());
		repository.save(Customer.builder().id("dave-003").firstname("Dave").lastname("CMatthews").build());
		repository.save(Customer.builder().id("dave-004").firstname("Dave").lastname("DMatthews").build());

		Iterable<Customer> customers = repository.findAllById(Arrays.asList("dave-001", "dave-004"));

		assertThat(customers).hasSize(2);
	}
}
