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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.core.AerospikeOperations;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.PersonRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class PersonRepositoryIntegrationTests extends BaseBlockingIntegrationTests {

	@Autowired
	PersonRepository repository;

	@Autowired
	AerospikeOperations operations;

	Person dave, donny, oliver, carter, boyd, stefan, leroi2, leroi, alicia;

	List<Person> all;

	@BeforeEach
	public void setUp() {
		super.setUp();
		blockingAerospikeTestOperations.deleteAll(Person.class);

		dave = Person.builder().id(nextId()).firstName("Dave").lastName("Matthews").age(42).build();
		donny = Person.builder().id(nextId()).firstName("Donny").lastName("Macintire").age(39).build();
		oliver = Person.builder().id(nextId()).firstName("Oliver August").lastName("Matthews").age(4).build();
		carter = Person.builder().id(nextId()).firstName("Carter").lastName("Beauford").age(49).build();
		boyd = Person.builder().id(nextId()).firstName("Boyd").lastName("Tinsley").age(45).build();
		stefan = Person.builder().id(nextId()).firstName("Stefan").lastName("Lessard").age(34).build();
		leroi = Person.builder().id(nextId()).firstName("Leroi").lastName("Moore").age(41).build();
		leroi2 = Person.builder().id(nextId()).firstName("Leroi").lastName("Moore").age(25).build();
		alicia = Person.builder().id(nextId()).firstName("Alicia").lastName("Keys").age(30).build();

		blockingAerospikeTestOperations.createIndexIfNotExists(Person.class, "person_last_name_index", "lastName", IndexType.STRING);
		blockingAerospikeTestOperations.createIndexIfNotExists(Person.class, "person_first_name_index", "firstName", IndexType.STRING);
		blockingAerospikeTestOperations.createIndexIfNotExists(Person.class, "person_age_index", "age", IndexType.NUMERIC);

		all = (List<Person>) repository.saveAll(Arrays.asList(oliver, dave, donny, carter, boyd, stefan, leroi, leroi2, alicia));
	}

	@Test
	public void findsPersonById() {
		Optional<Person> person = repository.findById(dave.getId());

		assertThat(person).hasValueSatisfying(actual -> {
			assertThat(actual).isInstanceOf(Person.class);
			assertThat(actual).isEqualTo(dave);
		});
	}

	@Test
	public void findsAllMusicians() {
		List<Person> result = (List<Person>) repository.findAll();

		assertThat(result)
				.hasSize(all.size())
				.containsAll(all);
	}

	@Test
	public void findsAllWithGivenIds() {
		List<Person> result = (List<Person>) repository.findAllById(Arrays.asList(dave.getId(), boyd.getId()));

		assertThat(result)
				.hasSize(2)
				.contains(dave)
				.doesNotContain(oliver, carter, stefan, leroi, alicia);
	}

	@Test
	public void findsPersonsByLastname() {
		List<Person> result = repository.findByLastName("Beauford");

		assertThat(result)
				.hasSize(1)
				.containsOnly(carter);
	}

	@Test
	public void deletesPersonCorrectly() {
		repository.delete(dave);

		List<Person> result = (List<Person>) repository.findAll();

		assertThat(result)
				.hasSize(all.size() - 1)
				.doesNotContain(dave);
	}

	@Test
	public void deletesPersonByIdCorrectly() {
		repository.deleteById(dave.getId());

		List<Person> result = (List<Person>) repository.findAll();

		assertThat(result)
				.hasSize(all.size() - 1)
				.doesNotContain(dave);
	}

	@Test
	public void findsPersonsByFirstname() {
		List<Person> result = repository.findByFirstName("Leroi");

		assertThat(result).hasSize(2).containsOnly(leroi, leroi2);
	}

	@Test
	public void findsByLastnameNot_forExistingResult() {
		Stream<Person> result = repository.findByLastNameNot("Moore");

		assertThat(result)
				.doesNotContain(leroi, leroi2)
				.contains(dave, donny, oliver, carter, boyd, stefan, alicia);
	}

	@Test
	public void findByFirstnameNotIn_forEmptyResult() {
		Set<String> allFirstNames = all.stream().map(p -> p.getFirstName()).collect(Collectors.toSet());
//		Stream<Person> result = repository.findByFirstnameNotIn(allFirstNames);
		assertThatThrownBy(() -> repository.findByFirstNameNotIn(allFirstNames))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Unsupported keyword!");

//		assertThat(result).isEmpty();
	}

	@Test
	public void findByFirstnameNotIn_forExistingResult() {
//		Stream<Person> result = repository.findByFirstnameNotIn(Collections.singleton("Alicia"));
		assertThatThrownBy(() -> repository.findByFirstNameNotIn(Collections.singleton("Alicia")))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Unsupported keyword!");

//		assertThat(result).contains(dave, donny, oliver, carter, boyd, stefan, leroi, leroi2);
	}

	@Test
	public void findByFirstnameIn_forEmptyResult() {
		Stream<Person> result = repository.findByFirstNameIn(Arrays.asList("Anastasiia", "Daniil"));

		assertThat(result).isEmpty();
	}

	@Test
	public void findByFirstnameIn_forExistingResult() {
		Stream<Person> result = repository.findByFirstNameIn(Arrays.asList("Alicia", "Stefan"));

		assertThat(result).contains(alicia, stefan);
	}

	@Test
	public void countByLastName_forExistingResult() {
		assertThatThrownBy(() -> repository.countByLastName("Leroi"))
				.isInstanceOf(UnsupportedOperationException.class)
				.hasMessage("Query method Person.countByLastName not supported.");

//		assertThat(result).isEqualTo(2);
	}

	@Test
	public void countByLastName_forEmptyResult() {
		assertThatThrownBy(() -> repository.countByLastName("Smirnova"))
				.isInstanceOf(UnsupportedOperationException.class)
				.hasMessage("Query method Person.countByLastName not supported.");

//		assertThat(result).isEqualTo(0);
	}

	@Test
	public void findByAgeGreaterThan_forExistingResult() {
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
	public void findByAgeGreaterThan_forEmptyResult() {
		Slice<Person> slice = repository.findByAgeGreaterThan(100, PageRequest.of(0, 10));

		assertThat(slice.hasContent()).isFalse();
		assertThat(slice.hasNext()).isFalse();
		assertThat(slice.getContent()).isEmpty();
	}

	@Test
	public void findByLastNameStartsWithOrderByAgeAsc_respectsLimitAndOffset() {
		Page<Person> first = repository.findByLastNameStartsWithOrderByAgeAsc("Moo", PageRequest.of(0, 1));

		assertThat(first.getNumberOfElements()).isEqualTo(1);
		assertThat(first.getTotalPages()).isEqualTo(2);
		assertThat(first.get()).hasSize(1).containsOnly(leroi2);

		Page<Person> last = repository.findByLastNameStartsWithOrderByAgeAsc("Moo", first.nextPageable());

		assertThat(last.getTotalPages()).isEqualTo(2);
		assertThat(last.getNumberOfElements()).isEqualTo(1);
		assertThat(last.get()).hasSize(1).containsAnyOf(leroi);

		Page<Person> all = repository.findByLastNameStartsWithOrderByAgeAsc("Moo", PageRequest.of(0, 5));

		assertThat(all.getTotalPages()).isEqualTo(1);
		assertThat(all.getNumberOfElements()).isEqualTo(2);
		assertThat(all.get()).hasSize(2).containsOnly(leroi, leroi2);
	}

	@Test
	public void findsPersonsByFirstnameAndByAge() {
		List<Person> result = repository.findByFirstNameAndAge("Leroi", 25);
		assertThat(result).containsOnly(leroi2);

		result = repository.findByFirstNameAndAge("Leroi", 41);
		assertThat(result).containsOnly(leroi);
	}

	@Test
	public void findsPersonsByFirstnameStartsWith() {
		List<Person> result = repository.findByFirstNameStartsWith("D");

		assertThat(result).containsOnly(dave, donny);
	}

	@Test
	public void findsPagedPersons() {
		Page<Person> result = repository.findAll(PageRequest.of(1, 2, Direction.ASC, "lastname", "firstname"));
		assertThat(result.isFirst()).isFalse();
		assertThat(result.isLast()).isFalse();
	}

	@Test
	public void findsPersonInAgeRangeCorrectly() {
		Iterable<Person> it = repository.findByAgeBetween(40, 45);

		assertThat(it).hasSize(3).contains(dave);
	}

	@Test
	public void findsPersonInAgeRangeCorrectlyOrderByLastName() {
		Iterable<Person> it = repository.findByAgeBetweenOrderByLastName(30, 45);

		assertThat(it).hasSize(6);
	}

	@Test
	public void findsPersonInAgeRangeAndNameCorrectly() {
		Iterable<Person> it = repository.findByAgeBetweenAndLastName(40, 45, "Matthews");
		assertThat(it).hasSize(1);

		Iterable<Person> result = repository.findByAgeBetweenAndLastName(20, 26, "Moore");
		assertThat(result).hasSize(1);
	}

//	@Ignore("Searching by association not Supported Yet!" )@Test
//	public void findsPersonByShippingAddressesCorrectly() {
//
//		Address address = new Address("Foo Street 1", "C0123", "Bar");
//		dave.setShippingAddresses(new HashSet<Address>(asList(address)));
//
//		repository.save(dave);
//		Person person = repository.findByShippingAddresses(address);
//		assertThat(repository.findByShippingAddresses(address), is(dave));
//	}

}
