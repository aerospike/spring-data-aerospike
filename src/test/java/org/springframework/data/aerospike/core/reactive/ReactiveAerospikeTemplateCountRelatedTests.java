package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.aerospike.repository.query.AerospikeCriteria;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.sample.Person;
import reactor.core.scheduler.Schedulers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for count related methods in {@link ReactiveAerospikeTemplate}.
 *
 * @author Igor Ermolenko
 */
public class ReactiveAerospikeTemplateCountRelatedTests extends BaseReactiveIntegrationTests {

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
        additionalAerospikeTestOperations.deleteAll(Person.class);
    }

    @Test
    public void count_shouldFindAllItemsByGivenCriteria() {
        reactiveTemplate.insert(new Person(nextId(), "vasili", 50)).block();
        reactiveTemplate.insert(new Person(nextId(), "vasili", 51)).block();
        reactiveTemplate.insert(new Person(nextId(), "vasili", 52)).block();
        reactiveTemplate.insert(new Person(nextId(), "petya", 52)).block();

        Qualifier.QualifierBuilder qbVasya1 = new Qualifier.QualifierBuilder()
                .setField("firstName")
                .setValue1(Value.get("vasili"))
                .setFilterOperation(FilterOperation.EQ);

        Query queryVasya1 = new Query(new AerospikeCriteria(qbVasya1));

        Long vasyaCount = reactiveTemplate.count(queryVasya1, Person.class)
                .subscribeOn(Schedulers.parallel())
                .block();
        assertThat(vasyaCount).isEqualTo(3);

        Qualifier.QualifierBuilder qbVasya2 = new Qualifier.QualifierBuilder()
                .setField("age")
                .setValue1(Value.get(51))
                .setFilterOperation(FilterOperation.EQ);

        Query queryVasyaAnd = new Query
            (new AerospikeCriteria
                    (new Qualifier.QualifierBuilder()
                            .setFilterOperation(FilterOperation.AND)
                            .setQualifiers(qbVasya1.build(), qbVasya2.build())
                    )
            );

        Long vasya51Count = reactiveTemplate.count(queryVasyaAnd, Person.class)
                .subscribeOn(Schedulers.parallel())
                .block();
        assertThat(vasya51Count).isEqualTo(1);

        Qualifier.QualifierBuilder qbPetya = new Qualifier.QualifierBuilder()
                .setField("firstName")
                .setValue1(Value.get("petya"))
                .setFilterOperation(FilterOperation.EQ);
        Long petyaCount = reactiveTemplate.count(new Query(new AerospikeCriteria(qbPetya)), Person.class)
                .subscribeOn(Schedulers.parallel())
                .block();
        assertThat(petyaCount).isEqualTo(1);
    }

    @Test
    public void count_shouldFindAllItemsByGivenCriteriaAndRespectsIgnoreCase() {
        reactiveTemplate.insert(new Person(nextId(), "VaSili", 50)).block();
        reactiveTemplate.insert(new Person(nextId(), "vasILI", 51)).block();
        reactiveTemplate.insert(new Person(nextId(), "vasili", 52)).block();

        Qualifier.QualifierBuilder qbVasya1 = new Qualifier.QualifierBuilder()
                .setField("firstName")
                .setValue1(Value.get("vas"))
                .setIgnoreCase(true)
                .setFilterOperation(FilterOperation.STARTS_WITH);

        Query query1 = new Query(new AerospikeCriteria(qbVasya1));
        assertThat(reactiveTemplate.count(query1, Person.class).block()).isEqualTo(3);

        Qualifier.QualifierBuilder qbVasya2 = new Qualifier.QualifierBuilder()
                .setField("firstName")
                .setValue1(Value.get("VaS"))
                .setIgnoreCase(false)
                .setFilterOperation(FilterOperation.STARTS_WITH);

        Query query2 = new Query(new AerospikeCriteria(qbVasya2));
        assertThat(reactiveTemplate.count(query2, Person.class)
                .subscribeOn(Schedulers.parallel())
                .block()).isEqualTo(1);
    }

    @Test
    public void count_shouldReturnZeroIfNoDocumentsByProvidedCriteriaIsFound() {
        Qualifier.QualifierBuilder qb1 = new Qualifier.QualifierBuilder()
                .setField("firstName")
                .setValue1(Value.get("nastyushka"))
                .setFilterOperation(FilterOperation.EQ);

        Long count = reactiveTemplate.count(new Query(new AerospikeCriteria(qb1)), Person.class)
                .subscribeOn(Schedulers.parallel())
                .block();

        assertThat(count).isZero();
    }

    @Test
    public void count_shouldRejectNullEntityClass() {
        assertThatThrownBy(() -> reactiveTemplate.count(null, (Class<?>) null).block())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void count_shouldCountAllByPassingEntityClass() {
        reactiveTemplate.insert(new Person(nextId(), "vasili", 50)).subscribeOn(Schedulers.parallel()).block();
        reactiveTemplate.insert(new Person(nextId(), "vasili", 51)).subscribeOn(Schedulers.parallel()).block();
        reactiveTemplate.insert(new Person(nextId(), "vasili", 52)).subscribeOn(Schedulers.parallel()).block();
        reactiveTemplate.insert(new Person(nextId(), "petya", 52)).subscribeOn(Schedulers.parallel()).block();

        Long count = reactiveTemplate.count(Person.class).block();
        assertThat(count).isEqualTo(4);
    }
}
