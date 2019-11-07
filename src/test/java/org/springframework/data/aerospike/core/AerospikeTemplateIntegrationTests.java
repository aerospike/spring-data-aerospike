/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.core;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.sample.Person;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Please do not add tests here. Instead add tests to AerospikeTemplateTests.
 * @author Oliver Gierke
 *
 */
//TODO: cleanup and move to AerospikeTemplateTests
@Deprecated
public class AerospikeTemplateIntegrationTests extends BaseIntegrationTests {

	protected static final String SET_NAME_PERSON = "Person";

	@Autowired AerospikeTemplate template;
	@Autowired AerospikeClient client;

	private WritePolicy policy = getWritePolicy();

	@Before
	public void cleanUp(){
		ScanPolicy scanPolicy = new ScanPolicy();
		scanPolicy.includeBinData = false;
		client.scanAll(	scanPolicy, getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON,
				(key, record) -> client.delete(null, key), new String[] {});
	}

	@Test
	public void testUpdate(){
		Person customer = new Person("dave-001", "Dave", "Matthews");
		template.insert(customer);
		String newLastName = customer.getLastname() + "xx";
		customer.setLastname(newLastName);
		template.update(customer);
		customer = template.findById("dave-001", Person.class);
		Assert.assertEquals("Matthewsxx", customer.getLastname());
	}

	@Test
	public void testInsert(){
		Person customer = new Person("dave-002", "Dave", "Matthews");
		template.insert(customer);
		Record result = client.get(null, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-002"));
		Assert.assertEquals("Dave", result.getString("firstname"));
		Assert.assertEquals("Matthews", result.getString("lastname"));
	}

	@Test
	public void testFindByKey(){
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-003"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));
		Person result = template.findById("dave-003", Person.class);
		Assert.assertEquals("Matthews", result.getLastname());
		Assert.assertEquals("Dave", result.getFirstname());
	}

	@Test
	public void testSingleIncrement(){
		Person customer = new Person("dave-002", "Dave", "Matthews");
		template.insert(customer);
		customer = template.add(customer, "age", 1);
		Record result = client.get(null, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-002"), "age");
		Assert.assertEquals(1, result.getInt("age"));
		Assert.assertEquals((Integer)result.getInt("age"), customer.getAge());
	}

	@Test
	public void testMultipleIncrement(){
		Person customer = new Person("dave-002", "Dave", "Matthews");
		template.insert(customer);
		Map<String, Long> values = new HashMap<String, Long>();
		values.put("age", 1L);

		customer = template.add(customer, values);
		Record result = client.get(null, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-002"), "age", "waist");
		Assert.assertEquals(1, result.getInt("age"));
		Assert.assertEquals((Integer)result.getInt("age"), customer.getAge());
	}

	@Test
	public void testDelete(){
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-001"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));
		Person customer = new Person("dave-001", "Dave", "Matthews");
		template.delete(customer);
		if ( client.exists(null, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-001")))
			Assert.fail("dave-001 was not deleted");
	}


	@Test
	public void testFindWithFilterEqual() {
		createIndexIfNotExists(Person.class, "first_name_index", "firstname", IndexType.STRING);

		template.insert(new Person("dave-001", "Dave", "Matthews"));
		template.insert(new Person("dave-002", "Dave", "Matthews"));
		template.insert(new Person("dave-003", "Dave", "Matthews"));
		template.insert(new Person("dave-004", "Dave", "Matthews"));
		template.insert(new Person("dave-005", "Dave", "Matthews"));
		template.insert(new Person("dave-006", "Dave", "Matthews"));
		template.insert(new Person("dave-007", "Dave", "Matthews"));
		template.insert(new Person("dave-008", "Dave", "Matthews"));
		template.insert(new Person("dave-009", "Dave", "Matthews"));
		template.insert(new Person("dave-010", "Dave", "Matthews"));

		Query query = createQueryForMethodWithArgs("findPersonByFirstname", "Dave");

		Stream<Person> result = template.find(query, Person.class);

		assertThat(result).hasSize(10);
	}

	@Test
	public void testFindWithFilterEqualOrderBy() {
		createIndexIfNotExists(Person.class, "age_index", "age", IndexType.NUMERIC);
		createIndexIfNotExists(Person.class, "last_name_index", "lastname", IndexType.STRING);

		template.insert(new Person("dave-001", "Jean", "Matthews", 21));
		template.insert(new Person("dave-002", "Ashley", "Matthews", 22));
		template.insert(new Person("dave-003", "Beatrice", "Matthews", 23));
		template.insert(new Person("dave-004", "Dave", "Matthews", 24));
		template.insert(new Person("dave-005", "Zaipper", "Matthews", 25));
		template.insert(new Person("dave-006", "knowlen", "Matthews", 26));
		template.insert(new Person("dave-007", "Xylophone", "Matthews", 27));
		template.insert(new Person("dave-008", "Mitch", "Matthews", 28));
		template.insert(new Person("dave-009", "Alister", "Matthews", 29));
		template.insert(new Person("dave-010", "Aabbbt", "Matthews", 30));

		Query query = createQueryForMethodWithArgs("findByLastnameOrderByFirstnameAsc","Matthews");

		Stream<Person> result = template.find(query, Person.class);

		assertThat(result).hasSize(10);
	}

	@Test
	public void testFindWithFilterEqualOrderByDesc() {
		createIndexIfNotExists(Person.class, "age_index", "age", IndexType.NUMERIC);
		createIndexIfNotExists(Person.class, "last_name_index", "lastname", IndexType.STRING);

		template.insert(new Person("dave-001", "Jean", "Matthews", 21));
		template.insert(new Person("dave-002", "Ashley", "Matthews", 22));
		template.insert(new Person("dave-003", "Beatrice", "Matthews", 23));
		template.insert(new Person("dave-004", "Dave", "Matthews", 24));
		template.insert(new Person("dave-005", "Zaipper", "Matthews", 25));
		template.insert(new Person("dave-006", "knowlen", "Matthews", 26));
		template.insert(new Person("dave-007", "Xylophone", "Matthews", 27));
		template.insert(new Person("dave-008", "Mitch", "Matthews", 28));
		template.insert(new Person("dave-009", "Alister", "Matthews", 29));
		template.insert(new Person("dave-010", "Aabbbt", "Matthews", 30));

		Object[] args = {"Matthews"};
		Query query = createQueryForMethodWithArgs("findByLastnameOrderByFirstnameDesc",args);

		Stream<Person> result = template.find(query, Person.class);

		assertThat(result).hasSize(10);
	}

	@Test
	public void testFindWithFilterRange() {
		createIndexIfNotExists(Person.class, "age_index", "age", IndexType.NUMERIC);

		template.insert(new Person("dave-001", "Dave01", "Matthews", 21));
		template.insert(new Person("dave-002", "Dave02", "Matthews", 22));
		template.insert(new Person("dave-003", "Dave03", "Matthews", 23));
		template.insert(new Person("dave-004", "Dave04", "Matthews", 24));
		template.insert(new Person("dave-005", "Dave05", "Matthews", 25));
		template.insert(new Person("dave-006", "Dave06", "Matthews", 26));
		template.insert(new Person("dave-007", "Dave07", "Matthews", 27));
		template.insert(new Person("dave-008", "Dave08", "Matthews", 28));
		template.insert(new Person("dave-009", "Dave09", "Matthews", 29));
		template.insert(new Person("dave-010", "Dave10", "Matthews", 30));

		Query query = createQueryForMethodWithArgs("findCustomerByAgeBetween", 25,30);

		Stream<Person> result = template.find(query, Person.class);

		assertThat(result).hasSize(6);
	}

	@Test
	public void testFindWithStatement() {
		createIndexIfNotExists(Person.class,"first_name_index", "firstname", IndexType.STRING);

		template.insert(new Person("dave-001", "Dave", "Matthews"));
		template.insert(new Person("dave-002", "Dave", "Matthews"));
		template.insert(new Person("dave-003", "Dave", "Matthews"));
		template.insert(new Person("dave-004", "Dave", "Matthews"));
		template.insert(new Person("dave-005", "Dave", "Matthews"));
		template.insert(new Person("dave-006", "Dave", "Matthews"));
		template.insert(new Person("dave-007", "Dave", "Matthews"));
		template.insert(new Person("dave-008", "Dave", "Matthews"));
		template.insert(new Person("dave-009", "Dave", "Matthews"));
		template.insert(new Person("dave-010", "Dave", "Matthews"));

		Statement aerospikeQuery = new Statement();
		String[] bins = {"firstname","lastname"}; //fields we want retrieved
		aerospikeQuery.setNamespace(getNameSpace()); // Database
		aerospikeQuery.setSetName(AerospikeTemplateIntegrationTests.SET_NAME_PERSON); //Table
		aerospikeQuery.setBinNames(bins);
		aerospikeQuery.setFilter(Filter.equal("firstname","Dave")); //Query

		RecordSet rs =  client.query(null, aerospikeQuery);
		int count = 0;
		while (rs.next()) {
			Record r = rs.getRecord();
			count++;
		}

		Assert.assertEquals(10, count);
	}

	private WritePolicy getWritePolicy() {
		WritePolicy policy = new WritePolicy();
		policy.sendKey = true;
		return policy;
	}

}

