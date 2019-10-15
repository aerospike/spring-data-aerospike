/*******************************************************************************
 * Copyright (c) 2018 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

package org.springframework.data.aerospike.repository;

import com.aerospike.client.query.IndexType;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.core.AerospikeOperations;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.Person.Sex;
import org.springframework.data.aerospike.sample.PersonRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class PersonRepositoryIntegrationTests extends BaseIntegrationTests {

	@Autowired
	protected PersonRepository repository;

	@Autowired
	AerospikeOperations operations;

	static int count = 0;

	Person dave, donny, oliver, carter, boyd, stefan, leroi2, leroi, alicia;

	List<Person> all, allKeyLess;

	@Before
	public void setUp() throws InterruptedException {
		repository.deleteAll();

		dave = new Person("Dave-01", "Dave", "Matthews", 42);
		donny = new Person("Dave-02", "Donny", "Macintire", 39);
		oliver = new Person("Oliver-01", "Oliver August", "Matthews", 4);
		carter = new Person("Carter-01", "Carter", "Beauford", 49);
		Thread.sleep(10);
		boyd = new Person("Boyd-01", "Boyd", "Tinsley", 45);
		stefan = new Person("Stefan-01", "Stefan", "Lessard", 34);
		leroi = new Person("Leroi-01", "Leroi", "Moore", 41);
		leroi2 = new Person("Leroi-02", "Leroi", "Moore", 25);
		alicia = new Person("Alicia-01", "Alicia", "Keys", 30, Sex.FEMALE);

		createIndexIfNotExists(Person.class, "last_name_index", "lastname", IndexType.STRING);
		createIndexIfNotExists(Person.class, "first_name_index", "firstname", IndexType.STRING);
		createIndexIfNotExists(Person.class, "person_age_index", "age", IndexType.NUMERIC);

		all = (List<Person>) repository.saveAll(Arrays.asList(oliver, dave, donny, carter, boyd, stefan, leroi, leroi2, alicia));
	}

	@Test
	public void findsPersonById() throws Exception {
		Optional<Person> person = repository.findById(dave.getId());

		assertThat(person).hasValueSatisfying(actual -> {
			assertThat(actual).isInstanceOf(Person.class);
			assertThat(actual).isEqualTo(dave);
		});
	}

	@Test
	public void findsAllMusicians() throws Exception {
		List<Person> result = (List<Person>) repository.findAll();
		assertThat(result.size(), is(all.size()));
		assertThat(result.containsAll(all), is(true));
	}

	@Test
	public void findsAllWithGivenIds() {
		List<Person> result = (List<Person>) repository.findAllById(Arrays.asList(dave.id, boyd.id));
		assertThat(result.size(), is(2));
		assertThat(result, hasItem(dave));
		assertThat(result, not(hasItems(oliver, carter, stefan, leroi, alicia)));
	}

	@Test
	public void findsPersonsByLastname() throws Exception {
		List<Person> result = repository.findByLastname("Beauford");
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(carter));
	}

	@Test
	public void deletesPersonCorrectly() throws Exception {
		repository.delete(dave);
		List<Person> result = (List<Person>) repository.findAll();
		assertThat(result.size(), is(all.size() - 1));
		assertThat(result, not(hasItem(dave)));
	}

	@Test
	public void deletesPersonByIdCorrectly() {
		repository.deleteById(dave.getId());
		List<Person> result = (List<Person>) repository.findAll();
		assertThat(result.size(), is(all.size() - 1));
		assertThat(result, not(hasItem(dave)));
	}

	@Test
	public void findsPersonsByFirstname() {
		List<Person> result = repository.findByFirstname("Leroi");

		assertThat(result).hasSize(2).containsOnly(leroi, leroi2);
	}

	@Test
	public void findsByLastnameNot_forExistingResult() throws Exception {
		Stream<Person> result = repository.findByLastnameNot("Moore");

		assertThat(result)
				.doesNotContain(leroi, leroi2)
				.contains(dave, donny, oliver, carter, boyd, stefan, alicia);
	}

	@Test
	public void findByFirstnameNotIn_forEmptyResult() throws Exception {
		Set<String> allFirstNames = all.stream().map(p -> p.getFirstname()).collect(Collectors.toSet());
//		Stream<Person> result = repository.findByFirstnameNotIn(allFirstNames);
		assertThatThrownBy(() -> repository.findByFirstnameNotIn(allFirstNames))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Unsupported keyword!");

//		assertThat(result).isEmpty();
	}

	@Test
	public void findByFirstnameNotIn_forExistingResult() throws Exception {
//		Stream<Person> result = repository.findByFirstnameNotIn(Collections.singleton("Alicia"));
		assertThatThrownBy(() -> repository.findByFirstnameNotIn(Collections.singleton("Alicia")))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Unsupported keyword!");

//		assertThat(result).contains(dave, donny, oliver, carter, boyd, stefan, leroi, leroi2);
	}

	@Test
	public void findByFirstnameIn_forEmptyResult() throws Exception {
		Stream<Person> result = repository.findByFirstnameIn(Arrays.asList("Anastasiia", "Daniil"));

		assertThat(result).isEmpty();
	}

	@Test
	public void findByFirstnameIn_forExistingResult() throws Exception {
		Stream<Person> result = repository.findByFirstnameIn(Arrays.asList("Alicia", "Stefan"));

		assertThat(result).contains(alicia, stefan);
	}

	@Test
	public void countByLastname_forExistingResult() throws Exception {
		assertThatThrownBy(() -> repository.countByLastname("Leroi"))
				.isInstanceOf(UnsupportedOperationException.class)
				.hasMessage("Query method Person.countByLastname not supported.");

//		assertThat(result).isEqualTo(2);
	}

	@Test
	public void countByLastname_forEmptyResult() throws Exception {
		assertThatThrownBy(() -> repository.countByLastname("Smirnova"))
				.isInstanceOf(UnsupportedOperationException.class)
				.hasMessage("Query method Person.countByLastname not supported.");

//		assertThat(result).isEqualTo(0);
	}

	@Test
	public void findByAgeGreaterThan_forExistingResult() throws Exception {
		Slice<Person> slice = repository.findByAgeGreaterThan(40, PageRequest.of(0, 10));

		assertThat(slice.hasContent()).isTrue();
		assertThat(slice.hasNext()).isFalse();
		assertThat(slice.getContent()).hasSize(4).contains(dave, carter, boyd, leroi);
	}

	@Test
	public void findByAgeGreaterThan_respectsLimit() {
		Slice<Person> slice = repository.findByAgeGreaterThan(40, PageRequest.of(0, 1));

		assertThat(slice.hasContent()).isTrue();
		assertThat(slice.hasNext()).isFalse();//TODO: not implemented yet. should be true instead
		assertThat(slice.getContent()).containsAnyOf(dave, carter, boyd, leroi).hasSize(1);
	}

	@Test
	public void findByAgeGreaterThan_respectsLimitAndOffsetAndSort() {
		List<Person> result = IntStream.range(0, 4)
				.mapToObj(index -> repository.findByAgeGreaterThan(40, PageRequest.of(index, 1, Sort.by("age"))))
				.flatMap(slice -> slice.getContent().stream())
				.collect(Collectors.toList());

		assertThat(result)
				.hasSize(4)
				.containsSequence(leroi, dave, boyd, carter);
	}

	@Test
	public void findByAgeGreaterThan_returnsValidValuesForNextAndPrev() {
		Slice<Person> first = repository.findByAgeGreaterThan(40, PageRequest.of(0, 1, Sort.by("age")));

		assertThat(first.hasContent()).isTrue();
		assertThat(first.getNumberOfElements()).isEqualTo(1);
		assertThat(first.hasNext()).isFalse();//TODO: not implemented yet. should be true instead
		assertThat(first.isFirst()).isTrue();

		Slice<Person> last = repository.findByAgeGreaterThan(40, PageRequest.of(3, 1, Sort.by("age")));

		assertThat(last.hasContent()).isTrue();
		assertThat(last.getNumberOfElements()).isEqualTo(1);
		assertThat(last.hasNext()).isFalse();
		assertThat(last.isLast()).isTrue();
	}

	@Test
	public void findByAgeGreaterThan_forEmptyResult() throws Exception {
		Slice<Person> slice = repository.findByAgeGreaterThan(100, PageRequest.of(0, 10));

		assertThat(slice.hasContent()).isFalse();
		assertThat(slice.hasNext()).isFalse();
		assertThat(slice.getContent()).isEmpty();
	}

	@Test
	public void findByLastnameStartsWithOrderByAgeAsc_respectsLimitAndOffset() {
		Page<Person> first = repository.findByLastnameStartsWithOrderByAgeAsc("Moo", PageRequest.of(0, 1));

		assertThat(first.getNumberOfElements()).isEqualTo(1);
		assertThat(first.getTotalPages()).isEqualTo(2);
		assertThat(first.get()).hasSize(1).containsOnly(leroi2);

		Page<Person> last = repository.findByLastnameStartsWithOrderByAgeAsc("Moo", first.nextPageable());

		assertThat(last.getTotalPages()).isEqualTo(2);
		assertThat(last.getNumberOfElements()).isEqualTo(1);
		assertThat(last.get()).hasSize(1).containsAnyOf(leroi);

		Page<Person> all = repository.findByLastnameStartsWithOrderByAgeAsc("Moo", PageRequest.of(0, 5));

		assertThat(all.getTotalPages()).isEqualTo(1);
		assertThat(all.getNumberOfElements()).isEqualTo(2);
		assertThat(all.get()).hasSize(2).containsOnly(leroi, leroi2);
	}

	@Test
	public void findsPersonsByFirstnameAndByAge() {
		List<Person> result = repository.findByFirstnameAndAge("Leroi", 25);
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(leroi2));
		assertThat(result.get(0).getAge(), is(25));
		result = repository.findByFirstnameAndAge("Leroi", 41);
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(leroi));
		assertThat(result.get(0).getAge(), is(41));
	}

	@Test
	public void findsPersonsByFirstnameStartsWith() throws Exception {
		List<Person> result = repository.findByFirstnameStartsWith("D");
		assertThat(result.size(), is(2));
		assertThat(result, hasItem(donny));
	}

	@Test
	public void findsPagedPersons() throws Exception {
		Page<Person> result = repository.findAll(PageRequest.of(1, 2, Direction.ASC, "lastname", "firstname"));
		assertThat(result.isFirst(), is(false));
		assertThat(result.isLast(), is(false));
	}

	@SuppressWarnings("unused")
	@Test
	public void findsPersonInAgeRangeCorrectly() throws Exception {
		Iterable<Person> it = repository.findByAgeBetween(40, 45);
		List<Person> result = repository.findByAgeBetween(40, 45);
		int count = 0;
		for (Person person : it) {
			count++;
		}
		assertEquals(3, count);
		assertThat(result, hasItem(dave));
	}

	@SuppressWarnings("unused")
	@Test
	public void findsPersonInAgeRangeCorrectlyOrderByLastname() throws Exception {
		Iterable<Person> it = repository.findByAgeBetweenOrderByLastname(30, 45);
		int count = 0;
		for (Person person : it) {
			count++;
		}
		assertEquals(6, count);
	}

	@SuppressWarnings("unused")
	@Test
	public void findsPersonInAgeRangeAndNameCorrectly() throws Exception {
		Iterable<Person> it = repository.findByAgeBetweenAndLastname(40, 45, "Matthews");
		Iterable<Person> result = repository.findByAgeBetweenAndLastname(20, 26, "Moore");
		int count = 0;
		for (Person person : it) {
			count++;
		}
		assertEquals(1, count);

		count = 0;
		for (Person person : result) {
			count++;
		}
		assertEquals(1, count);
	}

//	@Ignore("Searching by association not Supported Yet!" )@Test
//	public void findsPersonByShippingAddressesCorrectly() throws Exception {
//
//		Address address = new Address("Foo Street 1", "C0123", "Bar");
//		dave.setShippingAddresses(new HashSet<Address>(asList(address)));
//
//		repository.save(dave);
//		Person person = repository.findByShippingAddresses(address);
//		assertThat(repository.findByShippingAddresses(address), is(dave));
//	}

	@SuppressWarnings({ "serial", "unused" })
	@Test
	public void findsPersonByNameRetriveShippingAddressesCorrectly() throws Exception {
		HashMap<String, Object> myMap = new HashMap<String, Object>();
		myMap.put("Key", "String a ma thing");
		List<String> mySkills = new ArrayList<String>() {
			{
				add("Typing");
				add("Reading");
				add("Dungeons & Dragons");
			}
		};
		Address address = new Address("Shipping Address 1", "C0123", "Bar");
		Address mailingAddress = new Address("Shipping Address 2 Street 15555", "C0123", "Foo");
		HashSet<Address> shippingAddresses = new HashSet<Address>();
		shippingAddresses.add(address);
		shippingAddresses.add(mailingAddress);
		Address addressHome = new Address("Home Address 23", "C0123", "Bar");
		alicia.setAddress(addressHome);
		alicia.setShippingAddresses(shippingAddresses);
		alicia.setMyHashMap(myMap);
		alicia.setSkills(mySkills);

		repository.save(alicia);
		List<Person> result = repository.findByLastname(alicia.getLastname());
		count = 0;

		for (Person person : result) {
			count++;
		}
		assertEquals(1, count);
		assertThat(result.get(0).getShippingAddresses(), notNullValue());
		Person person = result.get(0);
		person.getShippingAddresses();
		//Address retShippingAddress = (Address) returnedAddressSet.toArray()[0];
		//assertThat(address.getZipCode(), is(retShippingAddress.getZipCode()));
		//assertThat(addressHome.getStreet(), is(person.getAddress().getStreet()));
	}

}
