package org.springframework.data.aerospike;

import com.aerospike.client.AerospikeClient;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.testcontainers.containers.GenericContainer;

public class ReactiveBlockingAerospikeTestOperations extends AdditionalAerospikeTestOperations {

    private final ReactiveAerospikeTemplate template;

    public ReactiveBlockingAerospikeTestOperations(AerospikeClient client, GenericContainer aerospike,
                                                   ReactiveAerospikeTemplate reactiveAerospikeTemplate) {
        super(client, aerospike);
        this.template = reactiveAerospikeTemplate;
    }

    @Override
    protected void delete(Class<?> clazz) {
        template.findAll(clazz)
                .flatMap(template::delete)
                .then().block();
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
