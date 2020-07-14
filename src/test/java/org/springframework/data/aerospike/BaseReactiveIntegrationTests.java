package org.springframework.data.aerospike;

import com.aerospike.client.reactor.AerospikeReactorClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.query.QueryEngine;
import org.springframework.data.aerospike.query.ReactorQueryEngine;
import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.Serializable;
import java.time.Duration;

public abstract class BaseReactiveIntegrationTests extends BaseIntegrationTests {

    @Autowired
    protected ReactiveAerospikeTemplate reactiveTemplate;
    @Autowired
    protected AerospikeReactorClient reactorClient;
    @Autowired
    protected ReactorQueryEngine reactorQueryEngine;

    protected <T> T findById(Serializable id, Class<T> type) {
        return reactiveTemplate.findById(id, type).block();
    }

    @BeforeAll
    public static void installBlockHound() {
        BlockHound.install();
    }

    @Test
    public void shouldFailAsBlocking(){
        StepVerifier.create(Mono.delay(Duration.ofSeconds(1))
                .doOnNext(it -> {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }))
                .expectError(BlockingOperationError.class)
                .verify();
    }

}