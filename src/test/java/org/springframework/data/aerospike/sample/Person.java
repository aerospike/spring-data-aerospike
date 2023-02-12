/*
 * Copyright 2012-2021 the original author or authors
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
 */
package org.springframework.data.aerospike.sample;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.aerospike.mapping.Field;
import org.springframework.data.annotation.Id;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Document
public class Person {

    private @Id String id;
    private String firstName;
    private String lastName;
    private int age;
    private int waist;
    private Sex sex;
    private Map<String, String> stringMap;
    private Map<String, Integer> intMap;
    private Person friend;
    private boolean active;
    private Date dateOfBirth;
    private List<String> strings;
    private List<Integer> ints;
    private Address address;
    @Field("email")
    private String emailAddress;
    public Person(String id, String firstName) {
        this.id = id;
        this.firstName = firstName;
    }

    public Person(String id, String firstName, String lastName) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public Person(String id, String firstName, int age) {
        this.id = id;
        this.firstName = firstName;
        this.age = age;
    }

    public PersonSomeFields toPersonSomeFields() {
        return PersonSomeFields.builder()
            .firstName(getFirstName())
            .lastName(getLastName())
            .emailAddress(getEmailAddress())
            .build();
    }

    public enum Sex {
        MALE, FEMALE
    }
}
