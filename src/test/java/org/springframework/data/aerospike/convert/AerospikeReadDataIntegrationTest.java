package org.springframework.data.aerospike.convert;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.annotation.Id;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class AerospikeReadDataIntegrationTest extends BaseBlockingIntegrationTests {

    long longId = 10L;
    String setName = "testSetName";
    String name = "John";
    int age = 74;
    Map<Integer, String> map = Map.of(10, "100");
    Address address = new Address("Street", 20, "ZipCode", "City");
    Map<String, Object> addressMap = Map.of("street", address.getStreet(), "apartment", 20, "zipCode", "ZipCode", "city", "City");

    @AllArgsConstructor
    @Getter
    static class User {
        @Id
        long id;
        String name;
        int age;
        Map<Integer, String> map;
        Address address;
    }

    @Test
    public void readLongId() {
        template.getAerospikeClient().put(null,
            new Key(namespace, template.getSetName(User.class), longId),
            new Bin("name", name),
            new Bin("age", 74),
            new Bin("map", map),
            new Bin("address", addressMap)
        );
        // we can read the record into a User document because its class is given
        List<User> users = template.findAll(User.class).toList();
        User user;
        if (template.getAerospikeConverter().getAerospikeDataSettings().isKeepOriginalKeyTypes()) {
            // the matching key is calculated if isKeepOriginalKeyTypes == true, otherwise returns null
            user = template.findById(longId, User.class);
            assertThat(users.get(0).getId() == (user.getId())).isTrue();
            assertThat(users.get(0).getName().equals(user.getName())).isTrue();
        } else {
            user = users.get(0);
        }
        assertThat(user.getId() == longId).isTrue();
        assertThat(user.getName().equals(name)).isTrue();
        assertThat(user.getAge() == (age)).isTrue();
        assertThat(user.getMap().equals(map)).isTrue();
        assertThat(user.getAddress().equals(address)).isTrue();
    }
}
