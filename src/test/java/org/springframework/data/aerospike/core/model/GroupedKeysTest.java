package org.springframework.data.aerospike.core.model;

import org.junit.Test;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.test.context.TestPropertySource;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.query.cache.IndexRefresher.INDEX_CACHE_REFRESH_SECONDS;

@TestPropertySource(properties = {INDEX_CACHE_REFRESH_SECONDS + " = 0", "createIndexesOnStartup = false"})
// this test class does not require secondary indexes created on startup
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
