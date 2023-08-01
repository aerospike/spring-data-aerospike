package org.springframework.data.aerospike.utility;

import org.springframework.data.aerospike.repository.AerospikeRepository;
import org.springframework.data.aerospike.repository.ReactiveAerospikeRepository;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.aerospike.sample.Person;

public class TestUtils {

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static void setFriendsToNull(AerospikeRepository repository, Person... persons) {
        for (Person person : persons) {
            person.setFriend(null);
            person.setBestFriend(null);
            repository.save(person);
        }
    }

    public static void setFriendsToNull(ReactiveAerospikeRepository<IndexedPerson, String> repository, IndexedPerson... persons) {
        for (IndexedPerson person : persons) {
            person.setFriend(null);
            person.setBestFriend(null);
            repository.save(person);
        }
    }
}
