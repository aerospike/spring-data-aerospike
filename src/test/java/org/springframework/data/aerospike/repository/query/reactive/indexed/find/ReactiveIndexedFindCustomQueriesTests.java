package org.springframework.data.aerospike.repository.query.reactive.indexed.find;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.Filter;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.annotation.Extensive;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.config.NoSecondaryIndexRequired;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.query.model.IndexKey;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.repository.query.reactive.indexed.ReactiveIndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.PersonId;
import org.springframework.data.aerospike.sample.PersonSomeFields;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;

import static com.aerospike.client.query.IndexType.NUMERIC;
import static com.aerospike.client.query.IndexType.STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeMetadata.SINCE_UPDATE_TIME;

@Nested
@Extensive
class ReactiveIndexedFindCustomQueriesTests extends ReactiveIndexedPersonRepositoryQueryTests {

    @Override
    protected List<Index> newIndexes() {
        List<Index> newIndexes = new ArrayList<>();
        String setName = reactiveTemplate.getSetName(IndexedPerson.class);
        String postfix = "r_find_custom";
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_first_name_" + postfix)
                .bin("firstName")
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
                .name("indexed_person_last_name_" + postfix)
                .bin("lastName")
                .indexType(STRING)
                .build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_gender_" + postfix)
                .bin("gender")
                .indexType(STRING)
                .build());
        return newIndexes;
    }

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
    void findByIdEquals_String_withProjection() {
        Qualifier idEquals = Qualifier.idEquals(petra.getId());
        Iterable<PersonId> results =
                reactiveRepository.findUsingQuery(new Query(idEquals), PersonId.class).collectList().block();
        assertThat(results).containsOnly(petra.toPersonId());
    }

    @Test
    void findBySimplePropertyEquals_String_withProjection() {
        String email = "emilien@test.com";
        emilien.setEmailAddress(email);
        reactiveRepository.save(emilien).block();

        Qualifier emailEquals = Qualifier.builder()
                // custom bin name has been set to "email" via @Field annotation
                .setPath("email")
                .setFilterOperation(FilterOperation.EQ)
                .setValue(email)
                .build();
        Iterable<PersonSomeFields> results =
                reactiveRepository.findUsingQuery(new Query(emailEquals), PersonSomeFields.class).collectList().block();
        assertThat(results).containsOnly(emilien.toPersonSomeFields());
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
                .setValue("Petra")
                .build();

        // creating an expression "age is equal to 34"
        Qualifier ageEq34 = Qualifier.builder()
                .setPath("age")
                .setFilterOperation(FilterOperation.EQ)
                .setValue(34)
                .build();
        result = reactiveRepository.findUsingQuery(new Query(ageEq34)).collectList().block();
        assertThat(result).containsOnly(petra);

        // creating an expression "age is greater than 34"
        Qualifier ageGt34 = Qualifier.builder()
                .setFilterOperation(FilterOperation.GT)
                .setPath("age")
                .setValue(34)
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

    @Test
    void findBySimpleProperty_AND() {
        Index firstNameIdx = indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "firstName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on firstName"));
        Index ageIdx = indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "age", NUMERIC, null)
        ).orElseThrow(() -> new RuntimeException("No index on age"));
        String binNameChosenForFilter = getBinNameForFilter(firstNameIdx, ageIdx);

        // creating an expression "firstName is equal to Emilien"
        Qualifier firstNameEq = Qualifier.builder()
                .setPath("firstName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("Emilien")
                .build();
        // creating an expression "age is equal to 30"
        Qualifier ageEq = Qualifier.builder()
                .setPath("age")
                .setFilterOperation(FilterOperation.EQ)
                .setValue(30)
                .build();

        Query query = new Query(Qualifier.and(firstNameEq, ageEq));
        Filter filter = getQuerySecIndexFilter(query, IndexedPerson.class);
        assertThat(filter).isNotNull();
        assertThat(filter.getName()).isEqualTo(binNameChosenForFilter);

        Expression expression = getQueryExpression(query, IndexedPerson.class);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.eq(Exp.bin("age", Exp.Type.INT), Exp.val((Integer) ageEq.getValue().getObject()))
                )
        );

        reactiveRepository.findUsingQuery(query).collectList()
                .as(StepVerifier::create)
                .assertNext(result -> assertThat(result).containsOnly(emilien))
                .verifyComplete();
    }

    @Test
    @AssertBinsAreIndexed(binNames = "firstName", entityClass = IndexedPerson.class)
    void findBySimpleProperty_AND_negative() {
        Qualifier firstNameEqJohn = Qualifier.builder()
                .setPath("firstName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("Emilien")
                .build();
        Qualifier firstNameEqPeter = Qualifier.builder()
                .setPath("firstName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("Daniel")
                .build();

        Query query = new Query(Qualifier.and(firstNameEqJohn, firstNameEqPeter));
        Filter filter = getQuerySecIndexFilter(query, IndexedPerson.class);
        assertThat(filter).isNotNull();
        assertThat(filter.getName()).isEqualTo("firstName");

        Expression expression = getQueryExpression(query, IndexedPerson.class);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.eq(Exp.bin("firstName", Exp.Type.STRING), Exp.val((String) firstNameEqPeter.getValue().getObject()))
                )
        );

        reactiveRepository.findUsingQuery(query).collectList()
                .as(StepVerifier::create)
                // First name cannot be simultaneously John and Peter
                .assertNext(result -> assertThat(result).isEmpty())
                .verifyComplete();
    }

    @Test
    @AssertBinsAreIndexed(binNames = "firstName", entityClass = IndexedPerson.class)
    void findBySimpleProperty_OR() {
        // creating an expression "firstName is equal to Emilien"
        Qualifier firstNameEqJohn = Qualifier.builder()
                .setPath("firstName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("Emilien")
                .build();
        // creating an expression "firstName is equal to Daniel"
        Qualifier firstNameEqPeter = Qualifier.builder()
                .setPath("firstName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("Daniel")
                .build();

        Query query = new Query(Qualifier.or(firstNameEqJohn, firstNameEqPeter));
        Filter filter = getQuerySecIndexFilter(query, IndexedPerson.class);
        // No single unifying secondary index Filter
        assertThat(filter).isNull();

        Expression expression = getQueryExpression(query, IndexedPerson.class);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.or(
                                Exp.eq(Exp.bin("firstName", Exp.Type.STRING), Exp.val((String) firstNameEqJohn.getValue().getObject())),
                                Exp.eq(Exp.bin("firstName", Exp.Type.STRING), Exp.val((String) firstNameEqPeter.getValue().getObject()))
                        )
                )
        );

        reactiveRepository.findUsingQuery(query).collectList()
                .as(StepVerifier::create)
                .assertNext(result -> assertThat(result).containsExactlyInAnyOrder(emilien, daniel))
                .verifyComplete();
    }

    @Test
    void findBySimpleProperty_AND_3elements() {
        Index firstNameIdx = indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "firstName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on firstName"));
        Index ageIdx = indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "age", NUMERIC, null)
        ).orElseThrow(() -> new RuntimeException("No index on age"));
        Index lastNameIdx = indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "lastName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on lastName"));
        String binNameChosenForFilter = getBinNameForFilter(firstNameIdx, ageIdx, lastNameIdx);

        Qualifier firstNameEq = Qualifier.builder()
                .setPath("firstName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("Emilien")
                .build();
        Qualifier ageEq = Qualifier.builder()
                .setPath("age")
                .setFilterOperation(FilterOperation.EQ)
                .setValue(30)
                .build();
        Qualifier lastNameEq = Qualifier.builder()
                .setPath("lastName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("Coutant-Kerbalec")
                .build();

        Query query = new Query(Qualifier.and(firstNameEq, ageEq, lastNameEq));
        Filter filter = getQuerySecIndexFilter(query, IndexedPerson.class);
        assertThat(filter).isNotNull();
        assertThat(filter.getName()).isEqualTo(binNameChosenForFilter);

        Expression expression = getQueryExpression(query, IndexedPerson.class);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.and(
                                Exp.eq(Exp.bin("age", Exp.Type.INT), Exp.val((Integer) ageEq.getValue().getObject())),
                                Exp.eq(Exp.bin("lastName", Exp.Type.STRING), Exp.val((String) lastNameEq.getValue().getObject()))
                        )
                )
        );

        reactiveRepository.findUsingQuery(query).collectList()
                .as(StepVerifier::create)
                .assertNext(result -> assertThat(result).containsOnly(emilien))
                .verifyComplete();
    }

    @Test
    void findBySimpleProperty_AND_AND() {
        Index firstNameIdx = indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "firstName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on firstName"));
        Index ageIdx = indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "age", NUMERIC, null)
        ).orElseThrow(() -> new RuntimeException("No index on age"));
        Index lastNameIdx = indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "lastName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on lastName"));
        String binNameChosenForFilter = getBinNameForFilter(firstNameIdx, ageIdx, lastNameIdx);

        Qualifier firstNameEq = Qualifier.builder()
                .setPath("firstName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("Emilien")
                .build();
        Qualifier ageEq = Qualifier.builder()
                .setPath("age")
                .setFilterOperation(FilterOperation.EQ)
                .setValue(30)
                .build();
        Qualifier lastNameEq = Qualifier.builder()
                .setPath("lastName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("Coutant-Kerbalec")
                .build();

        Query query = new Query(Qualifier.and(firstNameEq, Qualifier.and(ageEq, lastNameEq)));
        Filter filter = getQuerySecIndexFilter(query, IndexedPerson.class);
        assertThat(filter).isNotNull();
        assertThat(filter.getName()).isEqualTo(binNameChosenForFilter);

        Expression expression = getQueryExpression(query, IndexedPerson.class);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.and(
                                Exp.eq(Exp.bin("age", Exp.Type.INT), Exp.val((Integer) ageEq.getValue().getObject())),
                                Exp.eq(Exp.bin("lastName", Exp.Type.STRING), Exp.val((String) lastNameEq.getValue().getObject()))
                        )
                )
        );

        reactiveRepository.findUsingQuery(query).collectList()
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
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "gender", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on gender"));
        indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "lastName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on lastName"));
        // the cardinality of the corresponding index is higher (i.e. otherwise it would not have been chosen),
        // but based on the query only gender can be used for Filter because other bins are queried using OR
        String binNameChosenForFilter = "gender";

        Qualifier firstNameEq = Qualifier.builder()
                .setPath("firstName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("Emilien")
                .build();
        Qualifier genderEq = Qualifier.builder()
                .setPath("gender")
                .setFilterOperation(FilterOperation.EQ)
                .setValue(Person.Gender.MALE)
                .build();
        Qualifier lastNameEq = Qualifier.builder()
                .setPath("lastName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("Morales")
                .build();

        Query query = new Query(Qualifier.and(genderEq, Qualifier.or(firstNameEq, lastNameEq)));
        Filter filter = getQuerySecIndexFilter(query, IndexedPerson.class);
        assertThat(filter).isNotNull();
        assertThat(filter.getName()).isEqualTo(binNameChosenForFilter);

        Expression expression = getQueryExpression(query, IndexedPerson.class);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.or(
                                Exp.eq(Exp.bin("firstName", Exp.Type.STRING), Exp.val((String) firstNameEq.getValue().getObject())),
                                Exp.eq(Exp.bin("lastName", Exp.Type.STRING), Exp.val((String) lastNameEq.getValue().getObject()))
                        )
                )
        );

        reactiveRepository.findUsingQuery(query).collectList()
                .as(StepVerifier::create)
                .assertNext(result -> assertThat(result).containsExactlyInAnyOrder(emilien, daniel))
                .verifyComplete();
    }

    @Test
    @Disabled
    void findBySimpleProperty_OR_AND() {
        indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "firstName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on firstName"));
        indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "gender", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on gender"));
        indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "lastName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on lastName"));

        Qualifier firstNameEq = Qualifier.builder()
                .setPath("firstName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("Daniel")
                .build();
        Qualifier genderEq = Qualifier.builder()
                .setPath("gender")
                .setFilterOperation(FilterOperation.EQ)
                .setValue(Person.Gender.FEMALE)
                .build();
        Qualifier lastNameEq = Qualifier.builder()
                .setPath("lastName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("Morales")
                .build();

        Query query = new Query(Qualifier.or(genderEq, Qualifier.and(firstNameEq, lastNameEq)));
        Filter filter = getQuerySecIndexFilter(query, IndexedPerson.class);
        // No single uniting secondary index Filter
        assertThat(filter).isNull();

        Expression expression = getQueryExpression(query, IndexedPerson.class);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.or(
                                Exp.eq(Exp.bin("gender", Exp.Type.STRING), Exp.val((String) genderEq.getValue().getObject())),
                                Exp.and(
                                        Exp.eq(Exp.bin("firstName", Exp.Type.STRING), Exp.val((String) firstNameEq.getValue().getObject())),
                                        Exp.eq(Exp.bin("lastName", Exp.Type.STRING), Exp.val((String) lastNameEq.getValue().getObject()))
                                )
                        )
                )
        );

        reactiveRepository.findUsingQuery(query).collectList()
                .as(StepVerifier::create)
                .assertNext(result -> assertThat(result).containsExactlyInAnyOrder(daniel, lilly, petra))
                .verifyComplete();
    }

    @Test
    @Disabled
    void findBySimpleProperty_OR_OR() {
        indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "firstName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on firstName"));
        indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "gender", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on gender"));
        indexesCache.getIndex(
                new IndexKey(namespace, reactiveTemplate.getSetName(IndexedPerson.class), "lastName", STRING, null)
        ).orElseThrow(() -> new RuntimeException("No index on lastName"));

        Qualifier firstNameEq = Qualifier.builder()
                .setPath("firstName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("Emilien")
                .build();
        Qualifier genderEq = Qualifier.builder()
                .setPath("gender")
                .setFilterOperation(FilterOperation.EQ)
                .setValue(Person.Gender.FEMALE)
                .build();
        Qualifier lastNameEq = Qualifier.builder()
                .setPath("lastName")
                .setFilterOperation(FilterOperation.EQ)
                .setValue("Morales")
                .build();

        Query query = new Query(Qualifier.or(genderEq, Qualifier.or(firstNameEq, lastNameEq)));
        Filter filter = getQuerySecIndexFilter(query, IndexedPerson.class);
        // No single uniting secondary index Filter
        assertThat(filter).isNull();

        Expression expression = getQueryExpression(query, IndexedPerson.class);
        assertThat(expression).isEqualTo(
                Exp.build(
                        Exp.or(
                                Exp.eq(Exp.bin("gender", Exp.Type.STRING), Exp.val((String) genderEq.getValue().getObject())),
                                Exp.or(
                                        Exp.eq(Exp.bin("firstName", Exp.Type.STRING), Exp.val((String) firstNameEq.getValue().getObject())),
                                        Exp.eq(Exp.bin("lastName", Exp.Type.STRING), Exp.val((String) lastNameEq.getValue().getObject()))
                                )
                        )
                )
        );

        reactiveRepository.findUsingQuery(query).collectList()
                .as(StepVerifier::create)
                .assertNext(result -> assertThat(result).containsExactlyInAnyOrder(emilien, daniel, lilly, petra))
                .verifyComplete();
    }
}
