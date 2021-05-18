package org.springframework.data.aerospike.core.model;


import org.junit.Test;
import org.springframework.data.aerospike.sample.Person;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class GroupedKeysTest {

    @Test
    public void shouldGetEntitiesKeys() {
        Set<String> keys = new HashSet<>();
        keys.add("p22");

        GroupedKeys groupedKeys = GroupedKeys.builder()
                .entityKeys(Person.class, keys)
                .build();

        Map<Class<?>, Collection<?>> expectedResult =
                new HashMap<>();
        expectedResult.put(Person.class, keys);

        assertThat(groupedKeys.getEntitiesKeys()).containsAllEntriesOf(expectedResult);
    }
}
