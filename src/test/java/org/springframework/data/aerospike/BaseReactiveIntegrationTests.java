package org.springframework.data.aerospike;

import com.aerospike.client.reactor.AerospikeReactorClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.aerospike.config.CommonTestConfig;
import org.springframework.data.aerospike.config.ReactiveTestConfig;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.Serializable;
import java.time.Duration;

@SpringBootTest(
        classes = {ReactiveTestConfig.class, CommonTestConfig.class},
        properties = {
                "expirationProperty: 1",
                "setSuffix: service1"
        }
)
public abstract class BaseReactiveIntegrationTests extends BaseIntegrationTests {

    @Autowired
    protected ReactiveAerospikeTemplate reactiveTemplate;
    @Autowired
    protected AerospikeReactorClient reactorClient;

    protected <T> T findById(Serializable id, Class<T> type) {
        return reactiveTemplate.findById(id, type).block();
    }

    @BeforeAll
    public static void installBlockHound() {
        BlockHound.install();
    }

    @Test
    public void shouldFailAsBlocking() {
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