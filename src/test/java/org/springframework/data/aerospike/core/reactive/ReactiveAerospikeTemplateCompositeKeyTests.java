package org.springframework.data.aerospike.core.reactive;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.SampleClasses.CompositeKey;
import org.springframework.data.aerospike.SampleClasses.DocumentWithCompositeKey;
import reactor.test.StepVerifier;

import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class ReactiveAerospikeTemplateCompositeKeyTests extends BaseReactiveIntegrationTests {

    private DocumentWithCompositeKey document;

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
        document = new DocumentWithCompositeKey(new CompositeKey(nextId(), 77));
        reactiveTemplate.save(document).block();
    }

    @Test
    public void findById() {
        StepVerifier.create(reactiveTemplate.findById(document.getId(), DocumentWithCompositeKey.class))
                .expectNext(document)
                .verifyComplete();
    }

    @Test
    public void findByIds() {
        DocumentWithCompositeKey document2 = new DocumentWithCompositeKey(new CompositeKey(nextId(), 999));
        reactiveTemplate.save(document2).block();

        List<DocumentWithCompositeKey> actual = reactiveTemplate.findByIds(asList(document.getId(), document2.getId()), DocumentWithCompositeKey.class)
                .collectList().block();

        assertThat(actual).containsOnly(document, document2);
    }

    @Test
    public void delete() {
        StepVerifier.create(reactiveTemplate.delete(document.getId(), DocumentWithCompositeKey.class))
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    public void exists() {
        StepVerifier.create(reactiveTemplate.exists(document.getId(), DocumentWithCompositeKey.class))
                .expectNext(true)
                .verifyComplete();
    }
}