package org.springframework.data.aerospike.core;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.reactor.AerospikeReactorClient;
import com.aerospike.helper.query.Qualifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.aerospike.convert.AerospikeWriteData;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.aerospike.client.ResultCode.KEY_NOT_FOUND_ERROR;
import static java.util.Objects.nonNull;
import static org.springframework.data.aerospike.core.OperationUtils.operations;

/**
 * Primary implementation of {@link ReactiveAerospikeOperations}.
 *
 * @author Igor Ermolenko
 * @author Volodymyr Shpynta
 * @author Yevhen Tsyba
 */
@Slf4j
public class ReactiveAerospikeTemplate extends BaseAerospikeTemplate implements ReactiveAerospikeOperations {
    private final AerospikeReactorClient reactorClient;

    public ReactiveAerospikeTemplate(AerospikeClient client,
                                     String namespace,
                                     MappingAerospikeConverter converter,
                                     AerospikeMappingContext mappingContext,
                                     AerospikeExceptionTranslator exceptionTranslator,
                                     AerospikeReactorClient reactorClient) {
        super(client, namespace, converter, mappingContext, exceptionTranslator);
        Assert.notNull(reactorClient, "Aerospike reactor client must not be null!");
        this.reactorClient = reactorClient;
    }

    @Override
    public <T> Mono<T> save(T document) {
        Assert.notNull(document, "Object to save must not be null!");

        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            return doPersistWithCas(document, entity);
        } else {
            return doPersist(document, ignoreGeneration(RecordExistsAction.REPLACE));
        }
    }

    @Override
    public <T> Flux<T> insertAll(Collection<? extends T> documents) {
        return Flux.fromIterable(documents)
                .flatMap(this::insert);
    }

    @Override
    public <T> Mono<T> insert(T document) {
        Assert.notNull(document, "Document must not be null!");
        return doPersist(document, ignoreGeneration(RecordExistsAction.CREATE_ONLY));
    }

    @Override
    public <T> Mono<T> update(T document) {
        Assert.notNull(document, "Document must not be null!");
        return doPersist(document, ignoreGeneration(RecordExistsAction.UPDATE_ONLY));
    }

    @Override
    public <T> Flux<T> findAll(Class<T> type) {
        Stream<T> results = findAllUsingQuery(type, null, (Qualifier[]) null);
        return Flux.fromStream(results);
    }

    @Override
    public <T> Mono<T> add(T objectToAddTo, Map<String, Long> values) {
        Assert.notNull(objectToAddTo, "Object to add to must not be null!");
        Assert.notNull(values, "Values must not be null!");

        AerospikeWriteData data = writeData(objectToAddTo);

        Operation[] operations = new Operation[values.size() + 1];
        int x = 0;
        for (Map.Entry<String, Long> entry : values.entrySet()) {
            operations[x] = new Operation(Operation.Type.ADD, entry.getKey(), Value.get(entry.getValue()));
            x++;
        }
        operations[x] = Operation.get();

        WritePolicy writePolicy = new WritePolicy(this.client.writePolicyDefault);
        writePolicy.expiration = data.getExpiration();

        return executeOperationsOnValue(objectToAddTo, data, operations, writePolicy);
    }


    @Override
    public <T> Mono<T> add(T objectToAddTo, String binName, long value) {
        Assert.notNull(objectToAddTo, "Object to add to must not be null!");
        Assert.notNull(binName, "Bin name must not be null!");

        AerospikeWriteData data = writeData(objectToAddTo);

        WritePolicy writePolicy = new WritePolicy(this.client.writePolicyDefault);
        writePolicy.expiration = data.getExpiration();

        Operation[] operations = {Operation.add(new Bin(binName, value)), Operation.get(binName)};
        return executeOperationsOnValue(objectToAddTo, data, operations, writePolicy);
    }

    @Override
    public <T> Mono<T> append(T objectToAppendTo, Map<String, String> values) {
        Assert.notNull(objectToAppendTo, "Object to append to must not be null!");

        AerospikeWriteData data = writeData(objectToAppendTo);
        Operation[] operations = operations(values, Operation.Type.APPEND, Operation.get());
        return executeOperationsOnValue(objectToAppendTo, data, operations, null);
    }

    @Override
    public <T> Mono<T> append(T objectToAppendTo, String binName, String value) {
        Assert.notNull(objectToAppendTo, "Object to append to must not be null!");

        AerospikeWriteData data = writeData(objectToAppendTo);
        Operation[] operations = {Operation.append(new Bin(binName, value)), Operation.get(binName)};
        return executeOperationsOnValue(objectToAppendTo, data, operations, null);
    }

    @Override
    public <T> Mono<T> prepend(T objectToPrependTo, Map<String, String> values) {
        Assert.notNull(objectToPrependTo, "Object to prepend to must not be null!");

        AerospikeWriteData data = writeData(objectToPrependTo);
        Operation[] operations = operations(values, Operation.Type.PREPEND, Operation.get());
        return executeOperationsOnValue(objectToPrependTo, data, operations, null);
    }

    @Override
    public <T> Mono<T> prepend(T objectToPrependTo, String binName, String value) {
        Assert.notNull(objectToPrependTo, "Object to prepend to must not be null!");

        AerospikeWriteData data = writeData(objectToPrependTo);
        Operation[] operations = {Operation.prepend(new Bin(binName, value)), Operation.get(binName)};
        return executeOperationsOnValue(objectToPrependTo, data, operations, null);
    }

    @SuppressWarnings("unchecked")
    private <T> Mono<T> executeOperationsOnValue(T value, AerospikeWriteData data, Operation[] operations, WritePolicy writePolicy) {
        return reactorClient.operate(writePolicy, data.getKey(), operations)
                .filter(keyRecord -> Objects.nonNull(keyRecord.record))
                .map(keyRecord -> mapToEntity(keyRecord.key, (Class<T>) value.getClass(), keyRecord.record))
                .onErrorMap(this::translateError);
    }

    public <T> Mono<T> findById(Object id, Class<T> type) {
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(type);
        Key key = getKey(id, entity);

        if (entity.isTouchOnRead()) {
            Assert.state(!entity.hasExpirationProperty(), "Touch on read is not supported for entity without expiration property");
            return getAndTouch(key, entity.getExpiration())
                    .filter(keyRecord -> Objects.nonNull(keyRecord.record))
                    .map(keyRecord -> mapToEntity(keyRecord.key, type, keyRecord.record))
                    .onErrorResume(
                            th -> th instanceof AerospikeException && ((AerospikeException) th).getResultCode() == KEY_NOT_FOUND_ERROR,
                            th -> Mono.empty()
                    )
                    .onErrorMap(this::translateError);
        } else {
            return reactorClient.get(key)
                    .filter(keyRecord -> Objects.nonNull(keyRecord.record))
                    .map(keyRecord -> mapToEntity(keyRecord.key, type, keyRecord.record))
                    .onErrorMap(this::translateError);
        }
    }

    @Override
    public <T> Flux<T> findByIds(Iterable<?> ids, Class<T> type) {
        Assert.notNull(ids, "List of ids must not be null!");
        Assert.notNull(type, "Type must not be null!");

        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(type);

        return Flux.fromIterable(ids)
                .map(id -> getKey(id, entity))
                .flatMap(reactorClient::get)
                .filter(keyRecord -> nonNull(keyRecord.record))
                .map(keyRecord -> mapToEntity(keyRecord.key, type, keyRecord.record));
    }

    @Override
    public <T> Flux<T> find(Query query, Class<T> type) {
        Assert.notNull(query, "Query must not be null!");
        Assert.notNull(type, "Type must not be null!");

        Stream<T> results = findAllUsingQuery(type, query);
        return Flux.fromStream(results);
    }

    @Override
    public <T> Flux<T> findInRange(long offset, long limit, Sort sort, Class<T> type) {
        Assert.notNull(type, "Type for count must not be null!");
        Stream<T> results = findAllUsingQuery(type, null, (Qualifier[]) null)
                .skip(offset)
                .limit(limit);
        return Flux.fromStream(results);
    }

    @Override
    public <T> Mono<Long> count(Query query, Class<T> type) {
        Stream<KeyRecord> results = findAllRecordsUsingQuery(type, query);
        return Flux.fromStream(results).count();
    }

    @Override
    public <T> Mono<T> execute(Supplier<T> supplier) {
        Assert.notNull(supplier, "Callback must not be null!");
        return Mono.fromSupplier(supplier)
                .onErrorMap(this::translateError);
    }

    @Override
    public Mono<Boolean> exists(Object id, Class<?> type) {
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(type);
        Key key = getKey(id, entity);
        return reactorClient.exists(key)
                .map(Objects::nonNull)
                .defaultIfEmpty(false)
                .onErrorMap(this::translateError);
    }

    @Override
    public Mono<Boolean> delete(Object id, Class<?> type) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(type, "Type must not be null!");

        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(type);

        return reactorClient
                .delete(ignoreGeneration(), getKey(id, entity))
                .map(k -> true)
                .onErrorMap(this::translateError);
    }

    @Override
    public Mono<Boolean> delete(Object objectToDelete) {
        Assert.notNull(objectToDelete, "Object to delete must not be null!");

        AerospikeWriteData data = writeData(objectToDelete);

        return this.reactorClient
                .delete(ignoreGeneration(), data.getKey())
                .map(key -> true)
                .onErrorMap(this::translateError);
    }

    private <T> Mono<T> doPersist(T document, WritePolicyBuilder policyBuilder) {
        AerospikeWriteData data = writeData(document);
        WritePolicy policy = policyBuilder.expiration(data.getExpiration())
                .build();
        return reactorClient
                .put(policy, data.getKey(), data.getBinsAsArray())
                .map(docKey -> document)
                .onErrorMap(this::translateError);
    }

    private <T> Mono<T> doPersistWithCas(T document, AerospikePersistentEntity<?> entity) {
        AerospikeWriteData data = writeData(document);
        ConvertingPropertyAccessor accessor = getPropertyAccessor(entity, document);
        WritePolicy policy = getCasAwareWritePolicy(data);
        Operation[] operations = OperationUtils.operations(data.getBinsAsArray(), Operation::put, Operation.getHeader());
        return reactorClient.operate(policy, data.getKey(), operations)
                .map(newKeyRecord -> {
                    accessor.setProperty(entity.getVersionProperty(), newKeyRecord.record.generation);
                    return document;
                })
                .onErrorMap(AerospikeException.class, this::translateCasError);
    }

    private Mono<KeyRecord> getAndTouch(Key key, int expiration) {
        WritePolicy policy = new WritePolicy(client.writePolicyDefault);
        policy.expiration = expiration;
        return reactorClient.operate(policy, key, Operation.touch(), Operation.get());
    }

    private Throwable translateError(Throwable e) {
        if (e instanceof AerospikeException) {
            return translateError((AerospikeException) e);
        }
        return e;
    }

}
