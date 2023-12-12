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

public class AerospikeReadDataIntegrationTests extends BaseBlockingIntegrationTests {

    long longId = 10L;
    String setName = "testSetName";
    String name = "John";
    int age = 74;
    Map<Integer, String> map = Map.of(10, "100");
    Address address = new Address("Street", 20, "ZipCode", "City");
    Map<String, Object> addressMap = Map.of("street", address.getStreet(), "apartment", address.getApartment(),
        "zipCode", address.getZipCode(), "city", address.getCity());

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
    public void readDocumentWithLongId() {
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
            // we need isKeepOriginalKeyTypes == true because id is of type long, otherwise findById() returns null
            // isKeepOriginalKeyTypes parameter would be unimportant if id were of type String
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

    @AllArgsConstructor
    @Getter
    static class Document {

        @Id
        String id;
        int number;
    }

    @Test
    public void readLongIdAsString() {
        template.getAerospikeClient().put(null,
            new Key(namespace, template.getSetName(Document.class), longId),
            new Bin("number", age)
        );
        // we can read the record into a Document because its class is given
        List<Document> users = template.findAll(Document.class).toList();
        Document document;
        if (template.getAerospikeConverter().getAerospikeDataSettings().isKeepOriginalKeyTypes()) {
            // original id has type long
            document = template.findById(longId, Document.class);
            assertThat(users.get(0).getId().equals(document.getId())).isTrue();
            assertThat(users.get(0).getNumber() == (document.getNumber())).isTrue();
        } else {
            document = users.get(0);
        }
        assertThat(document.getId().equals(String.valueOf(longId))).isTrue();
        assertThat(document.getNumber() == age).isTrue();
    }
}
