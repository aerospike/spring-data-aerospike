package org.springframework.data.aerospike;

import com.aerospike.client.IAerospikeClient;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.query.cache.IndexInfoParser;
import org.testcontainers.containers.GenericContainer;

public class BlockingAerospikeTestOperations extends AdditionalAerospikeTestOperations {

    private final AerospikeTemplate template;

    public BlockingAerospikeTestOperations(IndexInfoParser indexInfoParser,
                                           AerospikeTemplate template,
                                           IAerospikeClient client,
                                           GenericContainer<?> aerospike) {
        super(indexInfoParser, client, aerospike);
        this.template = template;
    }

    @Override
    protected void delete(Class<?> clazz) {
        template.delete(clazz);
    }

    @Override
    protected String getNamespace() {
        return template.getNamespace();
    }

    @Override
    protected String getSetName(Class<?> clazz) {
        return template.getSetName(clazz);
    }
}
