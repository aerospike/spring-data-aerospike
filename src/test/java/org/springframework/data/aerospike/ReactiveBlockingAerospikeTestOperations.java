package org.springframework.data.aerospike;

import com.aerospike.client.IAerospikeClient;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.query.cache.IndexInfoParser;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.Person;
import org.testcontainers.containers.GenericContainer;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.springframework.data.aerospike.utility.AerospikeUniqueId.nextId;

public class ReactiveBlockingAerospikeTestOperations extends AdditionalAerospikeTestOperations {

    private final ReactiveAerospikeTemplate template;

    public ReactiveBlockingAerospikeTestOperations(IndexInfoParser indexInfoParser,
                                                   IAerospikeClient client, GenericContainer<?> aerospike,
                                                   ReactiveAerospikeTemplate reactiveAerospikeTemplate) {
        super(indexInfoParser, client, aerospike);
        this.template = reactiveAerospikeTemplate;
    }

    @Override
    protected boolean isEntityClassSetEmpty(Class<?> clazz) {
        return Boolean.FALSE.equals(template.findAll(clazz).hasElements().block());
    }

    @Override
    protected void truncateSetOfEntityClass(Class<?> clazz) {
        template.delete(clazz).block();
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
}
