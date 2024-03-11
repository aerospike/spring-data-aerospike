package org.springframework.data.aerospike;

import com.aerospike.client.reactor.IAerospikeReactorClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.aerospike.config.CommonTestConfig;
import org.springframework.data.aerospike.config.ReactiveTestConfig;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.aerospike.query.cache.ReactorIndexRefresher;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import reactor.core.publisher.Flux;

import java.io.Serializable;
import java.util.List;

import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeMetadata.LAST_UPDATE_TIME;

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
    protected ServerVersionSupport serverVersionSupport;
    @Autowired
    protected ReactorIndexRefresher reactorIndexRefresher;

    protected <T> T findById(Serializable id, Class<T> type) {
        return reactiveTemplate.findById(id, type).block();
    }

    protected <T> T findById(Serializable id, Class<T> type, String setName) {
        return reactiveTemplate.findById(id, type, setName).block();
    }

    protected <T> void deleteAll(Iterable<T> iterable) {
        Flux.fromIterable(iterable).flatMap(item -> reactiveTemplate.delete(item)).blockLast();
    }

    protected <T> void deleteAll(Iterable<T> iterable, String setName) {
        Flux.fromIterable(iterable).flatMap(item -> reactiveTemplate.delete(item, setName)).blockLast();
    }

    protected <T> List<T> runLastUpdateTimeQuery(long lastUpdateTimeMillis, FilterOperation operation,
                                                 Class<T> entityClass) {
        Qualifier lastUpdateTimeLtMillis = Qualifier.metadataBuilder()
            .setMetadataField(LAST_UPDATE_TIME)
            .setFilterOperation(operation)
            .setValueAsObj(lastUpdateTimeMillis * MILLIS_TO_NANO)
            .build();
        return reactiveTemplate.find(new Query(lastUpdateTimeLtMillis), entityClass).collectList().block();
    }
}
