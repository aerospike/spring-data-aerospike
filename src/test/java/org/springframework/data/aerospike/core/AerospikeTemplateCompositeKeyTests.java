package org.springframework.data.aerospike.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.SampleClasses.CompositeKey;
import org.springframework.data.aerospike.SampleClasses.DocumentWithCompositeKey;

import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class AerospikeTemplateCompositeKeyTests extends BaseBlockingIntegrationTests {

    private DocumentWithCompositeKey document;

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
        document = new DocumentWithCompositeKey(new CompositeKey(nextId(), 77));
        template.save(document);
    }

    @Test
    public void findById() {
        DocumentWithCompositeKey actual = template.findById(document.getId(), DocumentWithCompositeKey.class);

        assertThat(actual).isEqualTo(document);
    }

    @Test
    public void findByIds() {
        DocumentWithCompositeKey document2 = new DocumentWithCompositeKey(new CompositeKey("part1", 999));
        template.save(document2);

        List<DocumentWithCompositeKey> actual = template.findByIds(asList(document.getId(), document2.getId()), DocumentWithCompositeKey.class);

        assertThat(actual).containsOnly(document, document2);
    }

    @Test
    public void delete() {
        boolean deleted = template.delete(document.getId(), DocumentWithCompositeKey.class);

        assertThat(deleted).isTrue();
    }

    @Test
    public void exists() {
        boolean exists = template.exists(document.getId(), DocumentWithCompositeKey.class);

        assertThat(exists).isTrue();
    }
}