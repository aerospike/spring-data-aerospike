package org.springframework.data.aerospike.repository.query.reactive.indexed.findBy;

import com.aerospike.client.Value;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.config.NoSecondaryIndexRequired;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.repository.query.reactive.indexed.ReactiveIndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeMetadata.SINCE_UPDATE_TIME;

public class CustomQueriesTests extends ReactiveIndexedPersonRepositoryQueryTests {

    @Test
    @NoSecondaryIndexRequired
    public void findPersonsByMetadata() {
        // creating an expression "since_update_time metadata value is less than 50 seconds"
        Qualifier sinceUpdateTimeLt50Seconds = Qualifier.metadataBuilder()
            .setMetadataField(SINCE_UPDATE_TIME)
            .setFilterOperation(FilterOperation.LT)
            .setValue(50000L)
            .build();
        assertThat(reactiveRepository.findUsingQuery(new Query(sinceUpdateTimeLt50Seconds)).collectList().block())
            .containsAll(allIndexedPersons);

        // creating an expression "since_update_time metadata value is between 1 millisecond and 50 seconds"
        Qualifier sinceUpdateTimeBetween1And50000 = Qualifier.metadataBuilder()
            .setMetadataField(SINCE_UPDATE_TIME)
            .setFilterOperation(FilterOperation.BETWEEN)
            .setValue(1L)
            .setSecondValue(50000L)
            .build();
        assertThat(reactiveRepository.findUsingQuery(new Query(sinceUpdateTimeBetween1And50000)).collectList().block())
            .containsAll(reactiveRepository.findUsingQuery(new Query(sinceUpdateTimeLt50Seconds)).collectList()
                .block());
    }

    @Test
    @AssertBinsAreIndexed(binNames = {"firstName", "age",}, entityClass = IndexedPerson.class)
    public void findPersonsByQuery() {
        Iterable<IndexedPerson> result;

        // creating an expression "since_update_time metadata value is greater than 1 millisecond"
        Qualifier sinceUpdateTimeGt1 = Qualifier.metadataBuilder()
            .setMetadataField(SINCE_UPDATE_TIME)
            .setFilterOperation(FilterOperation.GT)
            .setValue(1L)
            .build();

        // creating an expression "since_update_time metadata value is less than 50 seconds"
        Qualifier sinceUpdateTimeLt50Seconds = Qualifier.metadataBuilder()
            .setMetadataField(SINCE_UPDATE_TIME)
            .setFilterOperation(FilterOperation.LT)
            .setValue(50000L)
            .build();
        assertThat(reactiveRepository.findUsingQuery(new Query(sinceUpdateTimeLt50Seconds)).collectList().block())
            .containsAll(allIndexedPersons);

        // creating an expression "since_update_time metadata value is between 1 and 50 seconds"
        Qualifier sinceUpdateTimeBetween1And50000 = Qualifier.metadataBuilder()
            .setMetadataField(SINCE_UPDATE_TIME)
            .setFilterOperation(FilterOperation.BETWEEN)
            .setValue(1L)
            .setSecondValue(50000L)
            .build();

        // creating an expression "firsName is equal to Petra"
        Qualifier firstNameEqPetra = Qualifier.builder()
            .setPath("firstName")
            .setFilterOperation(FilterOperation.EQ)
            .setValue(Value.get("Petra"))
            .build();

        // creating an expression "age is equal to 34"
        Qualifier ageEq34 = Qualifier.builder()
            .setPath("age")
            .setFilterOperation(FilterOperation.EQ)
            .setValue(Value.get(34))
            .build();
        result = reactiveRepository.findUsingQuery(new Query(ageEq34)).collectList().block();
        assertThat(result).containsOnly(petra);

        // creating an expression "age is greater than 34"
        Qualifier ageGt34 = Qualifier.builder()
            .setFilterOperation(FilterOperation.GT)
            .setPath("age")
            .setValue(Value.get(34))
            .build();
        result = reactiveRepository.findUsingQuery(new Query(ageGt34)).collectList().block();
        assertThat(result).doesNotContain(petra);

        result = reactiveRepository.findUsingQuery(new Query(Qualifier.and(sinceUpdateTimeGt1,
            sinceUpdateTimeLt50Seconds,
            ageEq34,
            firstNameEqPetra, sinceUpdateTimeBetween1And50000))).collectList().block();
        assertThat(result).containsOnly(petra);

        // conditions "age == 34", "firstName is Petra" and "since_update_time metadata value is less than 50 seconds"
        // are combined with OR
        Qualifier orWide = Qualifier.or(ageEq34, firstNameEqPetra, sinceUpdateTimeLt50Seconds);
        result = reactiveRepository.findUsingQuery(new Query(orWide)).collectList().block();
        assertThat(result).containsAll(allIndexedPersons);

        // conditions "age == 34" and "firstName is Petra" are combined with OR
        Qualifier orNarrow = Qualifier.or(ageEq34, firstNameEqPetra);
        result = reactiveRepository.findUsingQuery(new Query(orNarrow)).collectList().block();
        assertThat(result).containsOnly(petra);

        result = reactiveRepository.findUsingQuery(new Query(Qualifier.and(ageEq34, ageGt34))).collectList().block();
        assertThat(result).isEmpty();

        // conditions "age == 34" and "age > 34" are not overlapping
        result = reactiveRepository.findUsingQuery(new Query(Qualifier.and(ageEq34, ageGt34))).collectList().block();
        assertThat(result).isEmpty();

        // conditions "age == 34" and "age > 34" are combined with OR
        Qualifier ageEqOrGt34 = Qualifier.or(ageEq34, ageGt34);

        result = reactiveRepository.findUsingQuery(new Query(ageEqOrGt34)).collectList().block();
        List<IndexedPerson> personsWithAgeEqOrGt34 = allIndexedPersons.stream().filter(person -> person.getAge() >= 34)
            .toList();
        assertThat(result).containsAll(personsWithAgeEqOrGt34);

        // a condition that returns all entities and a condition that returns one entity are combined using AND
        result = reactiveRepository.findUsingQuery(new Query(Qualifier.and(orWide, orNarrow))).collectList().block();
        assertThat(result).containsOnly(petra);

        // a condition that returns all entities and a condition that returns one entity are combined using AND
        // another way of running the same query
        Qualifier orCombinedWithAnd = Qualifier.and(orWide, orNarrow);
        result = reactiveRepository.findUsingQuery(new Query(orCombinedWithAnd)).collectList().block();
        assertThat(result).containsOnly(petra);
    }
}

