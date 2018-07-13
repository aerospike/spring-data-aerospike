/**
 * 
 */
package org.springframework.data.aerospike.sample;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
//import java.util.stream.Stream;
import java.util.Set;

import org.springframework.data.aerospike.repository.AerospikeRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public interface PersonRepository extends AerospikeRepository<Person, String> {

	List<Person> findByLastname(String lastname);
	
	List<Person> findByLastnameStartsWith(String prefix);

	List<Person> findByLastnameEndsWith(String postfix);

	List<Person> findByLastnameOrderByFirstnameAsc(String lastname);
	
	List<Person> findByLastnameOrderByFirstnameDesc(String lastname);

	List<Person> findByFirstnameLike(String firstname);

	List<Person> findByFirstnameLikeOrderByLastnameAsc(String firstname, Sort sort);

	List<Person> findByAgeLessThan(int age, Sort sort);

	List<Person> findByFirstnameIn(String... firstnames);

	List<Person> findByFirstnameNotIn(Collection<String> firstnames);

	List<Person> findByFirstnameAndLastname(String firstname, String lastname);

	List<Person> findByAgeBetween(int from, int to);

	@SuppressWarnings("rawtypes")
	Person findByShippingAddresses(Set address);

	List<Person> findByAddress(Address address);

	List<Person> findByAddressZipCode(String zipCode);

	List<Person> findByLastnameLikeAndAgeBetween(String lastname, int from, int to);

	List<Person> findByAgeOrLastnameLikeAndFirstnameLike(int age, String lastname, String firstname);

	List<Person> findBySex(Person.Sex sex);

	List<Person> findBySex(Person.Sex sex, Pageable pageable);

//	List<Person> findByNamedQuery(String firstname);

	List<Person> findByCreator(User user);

	List<Person> findByCreatedAtLessThan(Date date);

	List<Person> findByCreatedAtGreaterThan(Date date);

//	List<Person> findByCreatedAtLessThanManually(Date date);

	List<Person> findByCreatedAtBefore(Date date);

	List<Person> findByCreatedAtAfter(Date date);

	List<Person> findByLastnameNot(String lastname);

	List<Person> findByCredentials(Credentials credentials);
	
	List<Person> findCustomerByAgeBetween(Integer from, Integer to);

	List<Person> findByAgeIn(ArrayList<Integer> ages);

	List<Person> findPersonByFirstname(String firstname);

	long countByLastname(String lastname);

	int countByFirstname(String firstname);

	long someCountQuery(String lastname);

	List<Person> findByFirstnameIgnoreCase(String firstName);

	List<Person> findByFirstnameNotIgnoreCase(String firstName);

	List<Person> findByFirstnameStartingWithIgnoreCase(String firstName);

	List<Person> findByFirstnameEndingWithIgnoreCase(String firstName);

	List<Person> findByFirstnameContainingIgnoreCase(String firstName);

	Slice<Person> findByAgeGreaterThan(int age, Pageable pageable);

	List<Person> deleteByLastname(String lastname);

	Long deletePersonByLastname(String lastname);

	Page<Person> findByAddressIn(List<Address> address, Pageable page);

	List<Person> findTop3ByLastnameStartingWith(String lastname);

	Page<Person> findTop3ByLastnameStartingWith(String lastname, Pageable pageRequest);

	List<Person> findByFirstname(String string);

	List<Person> findByFirstnameAndAge(String string, int i);

	Iterable<Person> findByAgeBetweenAndLastname(int from, int to, String lastname);

	List<Person> findByFirstnameStartsWith(String string);

	Iterable<Person> findByAgeBetweenOrderByLastname(int i, int j);

}
