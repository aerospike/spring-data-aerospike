package org.springframework.data.aerospike.core;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.helper.query.KeyRecordIterator;
import com.aerospike.helper.query.Qualifier;
import com.aerospike.helper.query.QueryEngine;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.support.PropertyComparator;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.aerospike.convert.AerospikeReadData;
import org.springframework.data.aerospike.convert.AerospikeTypeAliasAccessor;
import org.springframework.data.aerospike.convert.AerospikeWriteData;
import org.springframework.data.aerospike.convert.CustomConversions;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.mapping.AerospikeSimpleTypes;
import org.springframework.data.aerospike.mapping.BasicAerospikePersistentEntity;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.util.StreamUtils;
import org.springframework.util.Assert;
import org.springframework.util.comparator.CompoundComparator;

import java.util.Collections;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * Base class for creation Aerospike templates
 *
 * @author Anastasiia Smirnova
 * @author Igor Ermolenko
 */
@Slf4j
abstract class BaseAerospikeTemplate {

    protected final MappingContext<BasicAerospikePersistentEntity<?>, AerospikePersistentProperty> mappingContext;
    protected final AerospikeClient client;
    protected final MappingAerospikeConverter converter;
    protected final String namespace;
    protected final QueryEngine queryEngine;
    protected final AerospikeExceptionTranslator exceptionTranslator;

    BaseAerospikeTemplate(AerospikeClient client,
                          String namespace,
                          MappingAerospikeConverter converter,
                          AerospikeMappingContext mappingContext,
                          AerospikeExceptionTranslator exceptionTranslator) {
        Assert.notNull(client, "Aerospike client must not be null!");
        Assert.notNull(namespace, "Namespace cannot be null");
        Assert.hasLength(namespace, "Namespace cannot be empty");

        this.client = client;
        this.converter = converter;
        this.exceptionTranslator = exceptionTranslator;
        this.namespace = namespace;
        this.mappingContext = mappingContext;
        this.queryEngine = new QueryEngine(this.client);

        loggerSetup();
    }

    BaseAerospikeTemplate(AerospikeClient client, String namespace) {
        Assert.notNull(client, "Aerospike client must not be null!");
        Assert.notNull(namespace, "Namespace cannot be null");
        Assert.hasLength(namespace, "Namespace cannot be empty");

        CustomConversions customConversions = new CustomConversions(Collections.emptyList(), AerospikeSimpleTypes.HOLDER);
        AerospikeMappingContext asContext = new AerospikeMappingContext();
        asContext.setDefaultNameSpace(namespace);

        this.client = client;
        this.converter = new MappingAerospikeConverter(asContext, customConversions, new AerospikeTypeAliasAccessor());
        this.exceptionTranslator = new DefaultAerospikeExceptionTranslator();
        this.namespace = namespace;
        this.mappingContext = asContext;
        this.queryEngine = new QueryEngine(this.client);

        this.converter.afterPropertiesSet();

        loggerSetup();
    }

    private void loggerSetup() {
        Logger log = LoggerFactory.getLogger("com.aerospike.client");
        Log.setCallback((level, message) -> {
            switch (level) {
                case INFO:
                    log.info("{}", message);
                    break;
                case DEBUG:
                    log.debug("{}", message);
                    break;
                case ERROR:
                    log.error("{}", message);
                    break;
                case WARN:
                    log.warn("{}", message);
                    break;
            }
        });
    }

    public String getSetName(Class<?> entityClass) {
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);
        return entity.getSetName();
    }

    public MappingContext<?, ?> getMappingContext() {
        return this.mappingContext;
    }

    public String getNamespace() {
        return namespace;
    }

    <T> T mapToEntity(Key key, Class<T> type, Record record) {
        if (record == null) {
            return null;
        }
        AerospikeReadData data = AerospikeReadData.forRead(key, record);
        return converter.read(type, data);
    }

    <T> Stream<T> findAllUsingQuery(Class<T> type, Query query) {
        if ((query.getSort() == null || query.getSort().isUnsorted())
                && query.getOffset() > 0) {
            throw new IllegalArgumentException("Unsorted query must not have offset value. " +
                    "For retrieving paged results use sorted query.");
        }

        Qualifier qualifier = query.getCriteria().getCriteriaObject();
        Stream<T> results = findAllUsingQuery(type, null, qualifier);

        if (query.getSort() != null && query.getSort().isSorted()) {
            Comparator comparator = getComparator(query);
            results = results.sorted(comparator);
        }

        if(query.hasOffset()) {
            results = results.skip(query.getOffset());
        }
        if(query.hasRows()) {
            results = results.limit(query.getRows());
        }
        return results;
    }

    <T> Stream<T> findAllUsingQuery(Class<T> type, Filter filter, Qualifier... qualifiers) {
        return findAllRecordsUsingQuery(type, filter, qualifiers)
                .map(keyRecord -> mapToEntity(keyRecord.key, type, keyRecord.record));
    }

    <T> Stream<KeyRecord> findAllRecordsUsingQuery(Class<T> type, Filter filter, Qualifier... qualifiers) {
        String setName = getSetName(type);

        KeyRecordIterator recIterator = this.queryEngine.select(
                this.namespace, setName, filter, qualifiers);

        return StreamUtils.createStreamFromIterator(recIterator)
                .onClose(() -> {
                    try {
                        recIterator.close();
                    } catch (Exception e) {
                        log.error("Caught exception while closing query", e);
                    }
                });
    }

    <T> Stream<KeyRecord> findAllRecordsUsingQuery(Class<T> type, Query query) {
        Assert.notNull(query, "Query must not be null!");
        Assert.notNull(type, "Type must not be null!");

        Qualifier qualifier = query.getCriteria().getCriteriaObject();
        return findAllRecordsUsingQuery(type, null, qualifier);
    }

    private Comparator<?> getComparator(Query query) {
        //TODO replace with not deprecated one
        //TODO also see NullSafeComparator
        CompoundComparator<?> compoundComperator = new CompoundComparator();
        for (Sort.Order order : query.getSort()) {

            if (Sort.Direction.DESC.equals(order.getDirection())) {
                compoundComperator.addComparator(new PropertyComparator<>(order.getProperty(), true, false));
            }else {
                compoundComperator.addComparator(new PropertyComparator<>(order.getProperty(), true, true));
            }
        }

        return compoundComperator;
    }

    <T> ConvertingPropertyAccessor<T> getPropertyAccessor(AerospikePersistentEntity<?> entity, T source) {
        PersistentPropertyAccessor<T> accessor = entity.getPropertyAccessor(source);
        return new ConvertingPropertyAccessor<T>(accessor, converter.getConversionService());
    }

    RuntimeException translateCasError(AerospikeException e) {
        int code = e.getResultCode();
        if (code == ResultCode.KEY_EXISTS_ERROR || code == ResultCode.GENERATION_ERROR) {
            return new OptimisticLockingFailureException("Save document with version value failed", e);
        }
        return translateError(e);
    }

    RuntimeException translateError(AerospikeException e) {
        DataAccessException translated = exceptionTranslator.translateExceptionIfPossible(e);
        return translated == null ? e : translated;
    }

    <T> AerospikeWriteData writeData(T document) {
        AerospikeWriteData data = AerospikeWriteData.forWrite();
        converter.write(document, data);
        return data;
    }

    WritePolicy getCasAwareWritePolicy(AerospikeWriteData data) {
        Integer version = data.getVersion();
        boolean existingDocument = version != null && version > 0L;

        RecordExistsAction recordExistsAction;
        if (existingDocument) {
            //Updating existing document with generation
            recordExistsAction = RecordExistsAction.REPLACE_ONLY;
        } else {
            // create new document. if exists we should fail with optimistic locking
            recordExistsAction = RecordExistsAction.CREATE_ONLY;
        }
        return expectGeneration(data, recordExistsAction);
    }

    WritePolicy ignoreGeneration() {
        return WritePolicyBuilder.builder(this.client.writePolicyDefault)
                .generationPolicy(GenerationPolicy.NONE)
                .build();
    }

    WritePolicy expectGeneration(AerospikeWriteData data, RecordExistsAction recordExistsAction) {
        return WritePolicyBuilder.builder(this.client.writePolicyDefault)
                .generationPolicy(GenerationPolicy.EXPECT_GEN_EQUAL)
                .generation(data.getVersion() == null ? 0 : data.getVersion())
                .sendKey(true)
                .expiration(data.getExpiration())
                .recordExistsAction(recordExistsAction)
                .build();
    }

    WritePolicyBuilder ignoreGeneration(RecordExistsAction recordExistsAction) {
        return WritePolicyBuilder.builder(this.client.writePolicyDefault)
                .generationPolicy(GenerationPolicy.NONE)
                .sendKey(true)
                .recordExistsAction(recordExistsAction);
    }

    Key getKey(Object id, AerospikePersistentEntity<?> entity) {
        Assert.notNull(id, "Id must not be null!");
        return new Key(this.namespace, entity.getSetName(), id.toString());
    }

}
