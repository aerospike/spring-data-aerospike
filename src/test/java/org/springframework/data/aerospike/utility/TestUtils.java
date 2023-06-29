package org.springframework.data.aerospike.utility;

import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.PersonRepository;

public class TestUtils {

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static void setFriendsToNull(PersonRepository repository, Person... persons) {
        for (Person person : persons) {
            person.setFriend(null);
            person.setBestFriend(null);
            repository.save(person);
        }
    }
}
