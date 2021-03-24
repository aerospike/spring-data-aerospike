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
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;
import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.annotation.PersistenceConstructor;

@AllArgsConstructor(onConstructor_ = @PersistenceConstructor)
@EqualsAndHashCode(callSuper = true)
@SuperBuilder
@Document(collection = "indexed-person")
public class IndexedPerson extends Person {

    public static IndexedPerson from(Person person) {
        return IndexedPerson.builder()
                .id(person.getId())
                .firstName(person.getFirstName())
                .lastName(person.getLastName())
                .age(person.getAge())
                .waist(person.getWaist())
                .sex(person.getSex())
                .map(person.getMap())
                .friend(person.getFriend())
                .active(person.isActive())
                .dateOfBirth(person.getDateOfBirth())
                .strings(person.getStrings())
                .ints(person.getInts())
                .emailAddress(person.getEmailAddress())
                .build();
    }
}
