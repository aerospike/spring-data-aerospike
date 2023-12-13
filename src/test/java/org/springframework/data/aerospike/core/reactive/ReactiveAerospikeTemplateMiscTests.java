package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.core.WritePolicyBuilder;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.test.context.TestPropertySource;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.springframework.data.aerospike.query.cache.IndexRefresher.INDEX_CACHE_REFRESH_SECONDS;

/**
 * Tests for different methods in {@link ReactiveAerospikeTemplate}.
 *
 * @author Igor Ermolenko
 */
@TestPropertySource(properties = {INDEX_CACHE_REFRESH_SECONDS + " = 0", "createIndexesOnStartup = false"})
// this test class does not require secondary indexes created on startup
public class ReactiveAerospikeTemplateMiscTests extends BaseReactiveIntegrationTests {

    @Test
    public void execute_shouldTranslateException() {
        Key key = new Key(getNameSpace(), "shouldTranslateException", "reactiveShouldTranslateException");
        Bin bin = new Bin("bin_name", "bin_value");
        StepVerifier.create(reactorClient.add(null, key, bin))
            .expectNext(key)
            .verifyComplete();

        StepVerifier.create(reactiveTemplate.execute(() -> {
                WritePolicy writePolicy = WritePolicyBuilder.builder(reactorClient.getWritePolicyDefault())
                    .recordExistsAction(RecordExistsAction.CREATE_ONLY)
                    .build();
                return reactorClient.add(writePolicy, key, bin).subscribeOn(Schedulers.parallel()).block();
            }))
            .expectError(DuplicateKeyException.class)
            .verify();
    }

    @Test
    public void exists_shouldReturnTrueIfValueIsPresent() {
        Person one = Person.builder().id(id).firstName("tya").emailAddress("gmail.com").build();
        reactiveTemplate.insert(one).subscribeOn(Schedulers.parallel()).block();

        StepVerifier.create(reactiveTemplate.exists(id, Person.class).subscribeOn(Schedulers.parallel()))
            .expectNext(true)
            .verifyComplete();
        reactiveTemplate.delete(one).block();
    }

    @Test
    public void existsWithSetName_shouldReturnTrueIfValueIsPresent() {
        Person one = Person.builder().id(id).firstName("tya").emailAddress("gmail.com").build();
        reactiveTemplate.insert(one, OVERRIDE_SET_NAME).subscribeOn(Schedulers.parallel()).block();

        StepVerifier.create(reactiveTemplate.exists(id, OVERRIDE_SET_NAME).subscribeOn(Schedulers.parallel()))
            .expectNext(true)
            .verifyComplete();
        reactiveTemplate.delete(one, OVERRIDE_SET_NAME).block();
    }

    @Test
    public void exists_shouldReturnFalseIfValueIsAbsent() {
        StepVerifier.create(reactiveTemplate.exists(id, Person.class).subscribeOn(Schedulers.parallel()))
            .expectNext(false)
            .verifyComplete();
    }

    @Test
    public void existsWithSetName_shouldReturnFalseIfValueIsAbsent() {
        StepVerifier.create(reactiveTemplate.exists(id, OVERRIDE_SET_NAME).subscribeOn(Schedulers.parallel()))
            .expectNext(false)
            .verifyComplete();
    }
}
