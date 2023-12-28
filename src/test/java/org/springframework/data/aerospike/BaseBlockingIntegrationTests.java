package org.springframework.data.aerospike;

import com.aerospike.client.IAerospikeClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.aerospike.config.BlockingTestConfig;
import org.springframework.data.aerospike.config.CommonTestConfig;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.aerospike.query.QueryEngine;
import org.springframework.data.aerospike.query.cache.IndexRefresher;
import org.springframework.data.aerospike.query.cache.IndexesCache;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;

import java.util.Collection;
import java.util.List;

import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeMetadata.LAST_UPDATE_TIME;

@SpringBootTest(
    classes = {BlockingTestConfig.class, CommonTestConfig.class},
    properties = {
        "expirationProperty: 1",
        "setSuffix: service1",
        "indexSuffix: index1"
    }
)
public abstract class BaseBlockingIntegrationTests extends BaseIntegrationTests {

    @Autowired
    protected AerospikeTemplate template;
    @Autowired
    protected IAerospikeClient client;
    @Autowired
    protected QueryEngine queryEngine;
    @Autowired
    protected ServerVersionSupport serverVersionSupport;
    @Autowired
    protected IndexesCache indexesCache;
    @Autowired
    protected IndexRefresher indexRefresher;

    protected <T> void deleteOneByOne(Collection<T> collection) {
        collection.forEach(item -> template.delete(item));
    }

    protected <T> void deleteOneByOne(Collection<T> collection, String setName) {
        collection.forEach(item -> template.delete(item, setName));
    }

    protected <T> List<T> runLastUpdateTimeQuery(long lastUpdateTimeMillis, FilterOperation operation,
                                                 Class<T> entityClass) {
        Qualifier lastUpdateTimeLtMillis = Qualifier.metadataBuilder()
            .setMetadataField(LAST_UPDATE_TIME)
            .setFilterOperation(operation)
            .setValue1AsObj(lastUpdateTimeMillis * MILLIS_TO_NANO)
            .build();
        return template.find(new Query(lastUpdateTimeLtMillis), entityClass).toList();
    }
}
