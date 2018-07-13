/**
 * Copyright (c) 2015 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package org.springframework.data.aerospike.sample;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.aerospike.mapping.Field;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
@NoArgsConstructor
@Data
public class Person extends Contact implements Comparable<Person> {

	public enum Sex {
		MALE, FEMALE;
	}

	private String firstname;
	private HashMap<?,?> myHashMap;
	private String lastname;
	private String email;
	private Integer age;
	private Sex sex;

	private LocalDateTime createdAt;

	List<String> skills;
	private Address address;
	@Field(value = "ShipAddresses")
	private Set<Address> shippingAddresses;

	User creator;

	Credentials credentials;

	/**
	 * @param id
	 * @param firstname
	 * @param lastname
	 */
	public Person(String id, String firstname, String lastname) {

		this(id, firstname, lastname, null);
	}

	/**
	 * @param id
	 * @param firstname
	 * @param lastname
	 * @param age
	 */
	public Person(String id, String firstname, String lastname, Integer age) {

		this(id, firstname, lastname, age, Sex.MALE);
	}

	/**
	 * @param id
	 * @param firstname
	 * @param lastname
	 * @param age
	 * @param sex
	 */
	public Person(String id, String firstname, String lastname, Integer age, Sex sex) {
		super();
		this.id = id;
		this.firstname = firstname;
		this.lastname = lastname;
		this.age = age;
		this.sex = sex;
		this.email = (firstname == null ? "none" : firstname.toLowerCase()) + "@dmband.com";
		this.createdAt = LocalDateTime.now();
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Person another) {
		return this.lastname.compareTo(another.lastname);
	}

}
