package org.springframework.data.aerospike.repository.query.reactive.indexed.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.repository.query.reactive.indexed.ReactiveIndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;
import reactor.core.scheduler.Schedulers;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion.KEY;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion.KEY_VALUE_PAIR;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion.VALUE;

/**
 * Tests for the "Contains" repository query. Keywords: Containing, IsContaining, Contains.
 */
public class ContainingTests extends ReactiveIndexedPersonRepositoryQueryTests {

    @Test
    @AssertBinsAreIndexed(binNames = "strings", entityClass = IndexedPerson.class)
    public void findByCollectionContaining_String() {
        assertQueryHasSecIndexFilter("findByStringsContaining", IndexedPerson.class, "str1");
        List<IndexedPerson> results = reactiveRepository.findByStringsContaining("str1")
            .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(results).containsExactlyInAnyOrder(alain);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "ints", entityClass = IndexedPerson.class)
    public void findByCollectionContaining_Integer() {
        assertQueryHasSecIndexFilter("findByIntsContaining", IndexedPerson.class, 550);
        List<IndexedPerson> results = reactiveRepository.findByIntsContaining(550)
            .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(results).containsExactlyInAnyOrder(daniel, emilien);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "stringMap", entityClass = IndexedPerson.class)
    public void findByMapKeysContaining_String() {
        assertQueryHasSecIndexFilter("findByStringMapContaining", IndexedPerson.class, KEY, "key1");
        List<IndexedPerson> results = reactiveRepository.findByStringMapContaining(KEY, "key1")
            .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).contains(luc, petra);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "stringMap", entityClass = IndexedPerson.class)
    public void findByMapValuesContaining_String() {
        assertQueryHasSecIndexFilter("findByStringMapContaining", IndexedPerson.class, VALUE, "val1");
        List<IndexedPerson> results = reactiveRepository.findByStringMapContaining(VALUE, "val1")
            .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(results).contains(luc, petra);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "stringMap", entityClass = IndexedPerson.class)
    public void findByExactMapKeyAndValue_String() {
        assertThat(petra.getStringMap().containsKey("key1")).isTrue();
        assertThat(petra.getStringMap().containsValue("val1")).isTrue();
        assertThat(luc.getStringMap().containsKey("key1")).isTrue();
        assertThat(luc.getStringMap().containsValue("val1")).isTrue();
        assertQueryHasSecIndexFilter("findByStringMapContaining", IndexedPerson.class, KEY_VALUE_PAIR, "key1", "val1");

        List<IndexedPerson> results = reactiveRepository.findByStringMapContaining(KEY_VALUE_PAIR, "key1", "val1")
            .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(results).contains(petra, luc);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "intMap", entityClass = IndexedPerson.class)
    public void findByExactMapKeyAndValue_Integer() {
        assertThat(emilien.getIntMap().containsKey("key1")).isTrue();
        assertThat(emilien.getIntMap().get("key1")).isZero();
        assertThat(lilly.getIntMap().containsKey("key1")).isTrue();
        assertThat(lilly.getIntMap().get("key1")).isNotZero();
        assertQueryHasSecIndexFilter("findByIntMapContaining", IndexedPerson.class, KEY_VALUE_PAIR, "key1", 0);

        List<IndexedPerson> results = reactiveRepository.findByIntMapContaining(KEY_VALUE_PAIR, "key1", 0)
            .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(results).containsExactly(emilien);
    }
}
