/*
 * Copyright 2012-2018 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.core;

import java.util.List;
import java.util.Date;
import java.util.Map;

import lombok.*;
import org.springframework.data.aerospike.mapping.Field;
import org.springframework.data.annotation.Id;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
@AllArgsConstructor
@Getter
@Setter
@EqualsAndHashCode
@ToString
@Builder
public class Person {
	private @Id String id;
	private String firstName;
	private int age;
	private Map<String, String> map;
	private Person friend;
	private boolean active;
	private Date dateOfBirth;
	private List<String> list;

	@Field("email") 
	private String emailAddress;

	public Person() {
		super();
	}

	public Person(String id, String firstname) {
		this.id = id;
		this.firstName = firstname;
	}

	public Person(String id, String firstname, int age) {
		this.id = id;
		this.firstName = firstname;
		this.age = age;
	}

}
