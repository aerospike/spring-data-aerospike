package org.springframework.data.aerospike.repository.query.reactive.indexed.find;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.Filter;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.annotation.Nightly;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.query.model.IndexKey;
import org.springframework.data.aerospike.repository.query.reactive.indexed.ReactiveIndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;

import static com.aerospike.client.query.IndexCollectionType.MAPVALUES;
import static com.aerospike.client.query.IndexType.NUMERIC;
import static com.aerospike.client.query.IndexType.STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.query.QueryParam.of;

/**
 * Tests for the "Equals" repository query. Keywords: Is, Equals (or no keyword).
 */
@Nightly
public class ReactiveIndexedFindEqualsTests extends ReactiveIndexedPersonRepositoryQueryTests {

    @Override
    protected List<Index> newIndexes() {
        List<Index> newIndexes = new ArrayList<>();
        String setName = reactiveTemplate.getSetName(IndexedPerson.class);
        String postfix = "r_find_equals";
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_first_name_" + postfix)
                .bin("firstName")
                .indexType(STRING)
                .build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_last_name_" + postfix)
                .bin("lastName")
                .indexType(STRING)
                .build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_age_" + postfix)
                .bin("age")
                .indexType(NUMERIC)
                .build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_address_values_" + postfix)
                .bin("address")
                .indexType(STRING)
                .indexCollectionType(MAPVALUES)
                .build());
        return newIndexes;
    }

    @Test
    @AssertBinsAreIndexed(binNames = {"lastName", "firstName"}, entityClass = IndexedPerson.class)
    public void findBySimpleProperty_String() {
        assertQueryHasSecIndexFilter("findByLastName", IndexedPerson.class, "Coutant-Kerbalec");
        List<IndexedPerson> results = reactiveRepository.findByLastName("Coutant-Kerbalec")
                .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(results).containsOnly(petra, emilien);

        assertQueryHasSecIndexFilter("findByFirstName", IndexedPerson.class, "Lilly");
        List<IndexedPerson> results2 = reactiveRepository.findByFirstName("Lilly")
                .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(results2).containsExactlyInAnyOrder(lilly);
    }

    @Test
    @AssertBinsAreIndexed(binNames = {"firstName", "age"}, entityClass = IndexedPerson.class)
    public void findBySimpleProperty_String_AND_SimpleProperty_Integer() {
        QueryParam firstName = of("Lilly");
        QueryParam age = of(28);

        assertQueryHasSecIndexFilter("findByFirstNameAndAge", IndexedPerson.class, firstName, age);
        List<IndexedPerson> results = reactiveRepository.findByFirstNameAndAge(firstName, age)
                .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(results).containsOnly(lilly);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "address", entityClass = IndexedPerson.class)
    public void findByNestedSimpleProperty_String() {
        String zipCode = "C0123";
        assertThat(alain.getAddress().getZipCode()).isEqualTo(zipCode);
        assertQueryHasSecIndexFilter("findByAddressZipCode", IndexedPerson.class, zipCode);
        List<IndexedPerson> results = reactiveRepository.findByAddressZipCode(zipCode)
                .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(results).contains(alain);
    }

    @Test
    void findBySimpleProperty_AND() {
        Index firstNameIdx = indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "firstName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on firstName"));
        Index ageIdx = indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "age", NUMERIC, null)
        ).orElseThrow(() -> new RuntimeException("No index on age"));
        String binNameChosenForFilter = getBinNameForFilter(firstNameIdx, ageIdx);

        QueryParam firstName = of(emilien.getFirstName());
        QueryParam age = of(emilien.getAge());
        String queryName = "findByFirstNameAndAge";
        Filter filter = getQuerySecIndexFilter(queryName, IndexedPerson.class, firstName, age);
        assertThat(filter).isNotNull();
        assertThat(filter.getName()).isEqualTo(binNameChosenForFilter);

        Expression expression = getQueryExpression(queryName, IndexedPerson.class, firstName, age);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.eq(Exp.bin("age", Exp.Type.INT), Exp.val((Integer) age.arguments()[0]))
                )
        );

        reactiveRepository.findByFirstNameAndAge(firstName, age).collectList()
                .as(StepVerifier::create)
                .assertNext(result -> assertThat(result).containsOnly(emilien))
                .verifyComplete();
    }

    @Test
    void findBySimpleProperty_OR() {
        indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "firstName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on firstName"));
        indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "age", NUMERIC, null)
        ).orElseThrow(() -> new RuntimeException("No index on age"));

        QueryParam firstName = of(emilien.getFirstName());
        QueryParam age = of(daniel.getAge());
        String queryName = "findByFirstNameOrAge";
        Filter filter = getQuerySecIndexFilter(queryName, IndexedPerson.class, firstName, age);
        assertThat(filter).isNull();

        Expression expression = getQueryExpression(queryName, IndexedPerson.class, firstName, age);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.or(
                                Exp.eq(Exp.bin("firstName", Exp.Type.STRING), Exp.val((String) firstName.arguments()[0])),
                                Exp.eq(Exp.bin("age", Exp.Type.INT), Exp.val((Integer) age.arguments()[0]))
                        )
                )
        );

        reactiveRepository.findByFirstNameOrAge(firstName, age).collectList()
                .as(StepVerifier::create)
                .assertNext(result -> assertThat(result).containsExactlyInAnyOrder(emilien, daniel))
                .verifyComplete();
    }

    @Test
    void findBySimpleProperty_AND_AND() {
        indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "firstName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on firstName"));
        indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "age", NUMERIC, null)
        ).orElseThrow(() -> new RuntimeException("No index on age"));
        indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "lastName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on lastName"));
        // This qualifier gets processed first because the tree looks like AND(AND(age, firstName), lastName)
        String binNameChosenForFilter = "lastName";

        QueryParam firstName = of(emilien.getFirstName());
        QueryParam age = of(emilien.getAge());
        QueryParam lastName = of(emilien.getLastName());
        String queryName = "findByFirstNameAndAgeAndLastName";
        Filter filter = getQuerySecIndexFilter(queryName, IndexedPerson.class, firstName, age, lastName);
        assertThat(filter).isNotNull();
        assertThat(filter.getName()).isEqualTo(binNameChosenForFilter);

        Expression expression = getQueryExpression(queryName, IndexedPerson.class, firstName, age, lastName);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.and(
                                Exp.eq(Exp.bin("firstName", Exp.Type.STRING), Exp.val((String) firstName.arguments()[0])),
                                Exp.eq(Exp.bin("age", Exp.Type.INT), Exp.val((Integer) age.arguments()[0]))
                        )
                )
        );

        reactiveRepository.findByFirstNameAndAgeAndLastName(firstName, age, lastName).collectList()
                .as(StepVerifier::create)
                .assertNext(result -> assertThat(result).containsOnly(emilien))
                .verifyComplete();
    }

    @Test
    void findBySimpleProperty_AND_OR() {
        indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "firstName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on firstName"));
        indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "age", NUMERIC, null)
        ).orElseThrow(() -> new RuntimeException("No index on age"));
        indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "lastName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on lastName"));

        QueryParam firstName = of(emilien.getFirstName());
        QueryParam age = of(emilien.getAge());
        QueryParam lastName = of(daniel.getLastName());
        String queryName = "findByFirstNameAndAgeOrLastName";
        Filter filter = getQuerySecIndexFilter(queryName, IndexedPerson.class, firstName, age, lastName);
        assertThat(filter).isNull();

        Expression expression = getQueryExpression(queryName, IndexedPerson.class, firstName, age, lastName);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.or(
                                Exp.and(
                                        Exp.eq(Exp.bin("firstName", Exp.Type.STRING), Exp.val((String) firstName.arguments()[0])),
                                        Exp.eq(Exp.bin("age", Exp.Type.INT), Exp.val((Integer) age.arguments()[0]))
                                ),
                                Exp.eq(Exp.bin("lastName", Exp.Type.STRING), Exp.val((String) lastName.arguments()[0]))
                        )
                )
        );

        reactiveRepository.findByFirstNameAndAgeOrLastName(firstName, age, lastName).collectList()
                .as(StepVerifier::create)
                .assertNext(result -> assertThat(result).containsExactlyInAnyOrder(emilien, daniel))
                .verifyComplete();
    }

    @Test
    void findBySimpleProperty_OR_AND() {
        indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "firstName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on firstName"));
        indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "age", NUMERIC, null)
        ).orElseThrow(() -> new RuntimeException("No index on age"));
        indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "lastName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on lastName"));

        QueryParam firstName = of(emilien.getFirstName());
        QueryParam age = of(emilien.getAge());
        QueryParam lastName = of(daniel.getLastName());
        String queryName = "findByFirstNameOrAgeAndLastName";
        Filter filter = getQuerySecIndexFilter(queryName, IndexedPerson.class, firstName, age, lastName);
        assertThat(filter).isNull();

        Expression expression = getQueryExpression(queryName, IndexedPerson.class, firstName, age, lastName);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.or(
                                Exp.eq(Exp.bin("firstName", Exp.Type.STRING), Exp.val((String) firstName.arguments()[0])),
                                Exp.and(
                                        Exp.eq(Exp.bin("age", Exp.Type.INT), Exp.val((Integer) age.arguments()[0])),
                                        Exp.eq(Exp.bin("lastName", Exp.Type.STRING), Exp.val((String) lastName.arguments()[0]))
                                )
                        )
                )
        );

        reactiveRepository.findByFirstNameOrAgeAndLastName(firstName, age, lastName).collectList()
                .as(StepVerifier::create)
                .assertNext(result -> assertThat(result).containsOnly(emilien))
                .verifyComplete();
    }

    @Test
    void findBySimpleProperty_OR_OR() {
        indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "firstName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on firstName"));
        indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "age", NUMERIC, null)
        ).orElseThrow(() -> new RuntimeException("No index on age"));
        indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "lastName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on lastName"));

        QueryParam firstName = of(emilien.getFirstName());
        QueryParam age = of(petra.getAge());
        QueryParam lastName = of(daniel.getLastName());
        String queryName = "findByFirstNameOrAgeOrLastName";
        Filter filter = getQuerySecIndexFilter(queryName, IndexedPerson.class, firstName, age, lastName);
        assertThat(filter).isNull();

        Expression expression = getQueryExpression(queryName, IndexedPerson.class, firstName, age, lastName);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.or(
                                Exp.or(
                                        Exp.eq(Exp.bin("firstName", Exp.Type.STRING), Exp.val((String) firstName.arguments()[0])),
                                        Exp.eq(Exp.bin("age", Exp.Type.INT), Exp.val((Integer) age.arguments()[0]))
                                ),
                                Exp.eq(Exp.bin("lastName", Exp.Type.STRING), Exp.val((String) lastName.arguments()[0]))
                        )
                )
        );

        reactiveRepository.findByFirstNameOrAgeOrLastName(firstName, age, lastName).collectList()
                .as(StepVerifier::create)
                .assertNext(result -> assertThat(result).containsExactlyInAnyOrder(emilien, daniel, petra))
                .verifyComplete();
    }
}
