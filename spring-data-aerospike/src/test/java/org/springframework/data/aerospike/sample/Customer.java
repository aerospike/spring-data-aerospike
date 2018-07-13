/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.sample;

import java.util.UUID;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;

/**
 * @author Oliver Gierke
 */
@NoArgsConstructor
@Data
public class Customer {

	private  @Id String id;
	private  String firstname, lastname;
	private  long age = 0;

	/**
	 * @param firstname
	 * @param lastname
	 */
	public Customer(String firstname, String lastname) {
		this(UUID.randomUUID().toString(), firstname, lastname);
	}

	public Customer(String id, String firstname, String lastname) {
		this.id = id;
		this.firstname = firstname;
		this.lastname = lastname;
	}

}
