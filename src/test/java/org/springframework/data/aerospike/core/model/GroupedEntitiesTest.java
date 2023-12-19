package org.springframework.data.aerospike.core.model;

import org.junit.Test;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.test.context.TestPropertySource;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.query.cache.IndexRefresher.INDEX_CACHE_REFRESH_SECONDS;

@TestPropertySource(properties = {INDEX_CACHE_REFRESH_SECONDS + " = 0", "createIndexesOnStartup = false"})
// this test class does not require secondary indexes created on startup
public class GroupedEntitiesTest {

    private static final Map<Class<?>, Collection<?>> entitiesMap = Map.of(
        Person.class, List.of(Person.builder().id("22").build()),
        Customer.class, List.of(Customer.builder().id("33").build())
    );
    private static final GroupedEntities TEST_GROUPED_ENTITIES = GroupedEntities.builder()
        .entities(entitiesMap)
        .build();

    @Test
    public void shouldGetEntitiesByClass() {
        Person expectedResult = Person.builder().id("22").build();
        assertThat(TEST_GROUPED_ENTITIES.getEntitiesByClass(Person.class))
            .containsExactlyInAnyOrder(expectedResult);
    }

    @Test
    public void shouldReturnAnEmptyResultIfGroupedEntitiesDoesNotContainResult() {
        assertThat(TEST_GROUPED_ENTITIES.getEntitiesByClass(String.class)).isEmpty();
    }

    @Test
    public void shouldContainEntities() {
        assertThat(TEST_GROUPED_ENTITIES.containsEntities()).isTrue();
    }

    @Test
    public void shouldNotContainEntities() {
        GroupedEntities groupedEntities = GroupedEntities.builder().build();
        assertThat(groupedEntities.containsEntities()).isFalse();
    }
}
