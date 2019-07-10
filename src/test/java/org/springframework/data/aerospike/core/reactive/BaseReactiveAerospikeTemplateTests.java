package org.springframework.data.aerospike.core.reactive;

import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;

import java.io.Serializable;

/**
 * Base class for implementation tests for {@link AerospikeTemplate}.
 *
 * @author Igor Ermolenko
 */
public abstract class BaseReactiveAerospikeTemplateTests extends BaseIntegrationTests {
    @Autowired
    protected ReactiveAerospikeTemplate reactiveTemplate;

    String id;

    @Before
    public void setUp() {
        this.id = nextId();
        cleanDb();
    }

    <T> T findById(Serializable id, Class<T> type) {
        return reactiveTemplate.findById(id, type).block();
    }

}
