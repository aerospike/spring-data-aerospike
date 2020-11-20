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
