package org.springframework.data.aerospike;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.query.cache.IndexInfoParser;
import org.springframework.data.aerospike.repository.ReactiveAerospikeRepository;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.utility.AdditionalAerospikeTestOperations;
import org.springframework.data.aerospike.utility.ServerVersionUtils;
import org.testcontainers.containers.GenericContainer;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.springframework.data.aerospike.utility.AerospikeUniqueId.nextId;

public class ReactiveBlockingAerospikeTestOperations extends AdditionalAerospikeTestOperations {

    private final ReactiveAerospikeTemplate template;

    public ReactiveBlockingAerospikeTestOperations(IndexInfoParser indexInfoParser,
                                                   IAerospikeClient client, GenericContainer<?> aerospike,
                                                   ReactiveAerospikeTemplate reactiveAerospikeTemplate) {
        super(indexInfoParser, client, reactiveAerospikeTemplate, aerospike);
        this.template = reactiveAerospikeTemplate;
    }

    @Override
    protected boolean isEntityClassSetEmpty(Class<?> clazz) {
        return Boolean.FALSE.equals(template.findAll(clazz).hasElements().block());
    }

    @Override
    protected void truncateSetOfEntityClass(Class<?> clazz) {
        template.deleteAll(clazz).block();
    }

    @Override
    protected boolean isSetEmpty(Class<?> clazz, String setName) {
        return Boolean.FALSE.equals(template.findAll(clazz, setName).hasElements().block());
    }

    @Override
    protected void truncateSet(String setName) {
        template.deleteAll(setName).block();
    }

    @Override
    protected String getNamespace() {
        return template.getNamespace();
    }

    @Override
    protected String getSetName(Class<?> clazz) {
        return template.getSetName(clazz);
    }

    public List<Customer> generateCustomers(int count) {
        return IntStream.range(0, count)
            .mapToObj(i -> Customer.builder().id(nextId())
                .firstName("firstName" + i)
                .lastName("lastName")
                .build())
            .peek(template::save)
            .collect(Collectors.toList());
    }

    public List<Person> generatePersons(int count) {
        return IntStream.range(0, count)
            .mapToObj(i -> Person.builder().id(nextId())
                .firstName("firstName" + i)
                .emailAddress("mail.com")
                .build())
            .peek(template::save)
            .collect(Collectors.toList());
    }

    public <T> void deleteAll(ReactiveAerospikeRepository<T, ?> repository, Collection<T> entities) {
        // batch write operations are supported starting with Server version 6.0+
        if (ServerVersionUtils.isBatchWriteSupported(template.getAerospikeReactorClient().getAerospikeClient())) {
            try {
                repository.deleteAll(entities).block();
            } catch (AerospikeException.BatchRecordArray ignored) {
                // KEY_NOT_FOUND ResultCode causes exception if there are no entities
            }
        } else {
            entities.forEach(entity -> repository.delete(entity).block());
        }
    }

    public <T> void saveAll(ReactiveAerospikeRepository<T, ?> repository, Collection<T> entities) {
        // batch write operations are supported starting with Server version 6.0+
        if (ServerVersionUtils.isBatchWriteSupported(template.getAerospikeReactorClient().getAerospikeClient())) {
            repository.saveAll(entities).blockLast();
        } else {
            entities.forEach(entity -> repository.save(entity).block());
        }
    }
}
