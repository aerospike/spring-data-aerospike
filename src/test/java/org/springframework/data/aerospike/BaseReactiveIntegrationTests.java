package org.springframework.data.aerospike;

import com.aerospike.client.reactor.IAerospikeReactorClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.aerospike.config.CommonTestConfig;
import org.springframework.data.aerospike.config.ReactiveTestConfig;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.query.cache.ReactorIndexRefresher;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.utility.ServerVersionUtils;
import reactor.core.publisher.Flux;

import java.io.Serializable;
import java.util.Collection;

@SpringBootTest(
    classes = {ReactiveTestConfig.class, CommonTestConfig.class},
    properties = {
        "expirationProperty: 1",
        "setSuffix: service1",
        "indexSuffix: index1"
    }
)
public abstract class BaseReactiveIntegrationTests extends BaseIntegrationTests {

    @Autowired
    protected ReactiveAerospikeTemplate reactiveTemplate;
    @Autowired
    protected IAerospikeReactorClient reactorClient;
    @Autowired
    protected ServerVersionUtils serverVersionUtils;
    @Autowired
    protected ReactorIndexRefresher reactorIndexRefresher;

    protected <T> T findById(Serializable id, Class<T> type) {
        return reactiveTemplate.findById(id, type).block();
    }

    protected <T> T findById(Serializable id, Class<T> type, String setName) {
        return reactiveTemplate.findById(id, type, setName).block();
    }

    protected void deleteAll(Collection<Person> persons) {
        Flux.fromIterable(persons).flatMap(person -> reactiveTemplate.delete(person)).blockLast();
    }

    protected void deleteAll(Collection<Person> persons, String setName) {
        Flux.fromIterable(persons).flatMap(person -> reactiveTemplate.delete(person, setName)).blockLast();
    }
}
