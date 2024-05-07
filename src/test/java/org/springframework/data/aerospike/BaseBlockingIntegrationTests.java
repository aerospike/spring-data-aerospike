package org.springframework.data.aerospike;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.query.Statement;
import org.junit.jupiter.api.TestInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.data.aerospike.config.BlockingTestConfig;
import org.springframework.data.aerospike.config.CommonTestConfig;
import org.springframework.data.aerospike.config.IndexedBinsAnnotationsProcessor;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.mapping.BasicAerospikePersistentEntity;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.QueryEngine;
import org.springframework.data.aerospike.query.cache.IndexRefresher;
import org.springframework.data.aerospike.query.cache.IndexesCache;
import org.springframework.data.aerospike.query.model.IndexedField;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.aerospike.util.QueryUtils;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.StringUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.function.Predicate.not;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.config.IndexedBinsAnnotationsProcessor.getBinNames;
import static org.springframework.data.aerospike.config.IndexedBinsAnnotationsProcessor.getEntityClass;
import static org.springframework.data.aerospike.config.IndexedBinsAnnotationsProcessor.hasAssertBinsAreIndexedAnnotation;
import static org.springframework.data.aerospike.core.TemplateUtils.getBinNamesFromTargetClass;
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
    @Autowired
    protected Environment env;
    @Autowired
    protected MappingContext<BasicAerospikePersistentEntity<?>, AerospikePersistentProperty> mappingContext;

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
            .setValueAsObj(lastUpdateTimeMillis * MILLIS_TO_NANO)
            .build();
        return template.find(new Query(lastUpdateTimeLtMillis), entityClass).toList();
    }

    protected boolean isIndexedBin(String namespace, String setName, String binName) {
        boolean hasIndex = false;
        if (StringUtils.hasLength(binName)) {
            hasIndex = indexesCache.hasIndexFor(
                new IndexedField(namespace, setName, binName)
            );
        }
        return hasIndex;
    }

    protected void assertBinIsIndexed(String binName, Class<?> clazz) {
        assertThat(isIndexedBin(getNameSpace(), template.getSetName(clazz), binName))
            .as(String.format("Expecting bin %s to be indexed", binName)).isTrue();
    }

    protected void assertBinsAreIndexed(TestInfo testInfo) {
        testInfo.getTestMethod().stream()
            .filter(not(IndexedBinsAnnotationsProcessor::hasNoindexAnnotation))
            .forEach(testMethod -> {
                assertThat(hasAssertBinsAreIndexedAnnotation(testMethod))
                    .as(String.format("Expecting the test method %s to have @AssertBinsAreIndexed annotation",
                        testMethod.getName()))
                    .isTrue();
                String[] binNames = getBinNames(testMethod);
                Class<?> entityClass = getEntityClass(testMethod);
                assertThat(binNames).as("Expecting bin names to be populated").isNotNull();
                assertThat(binNames).as("Expecting bin names ").isNotEmpty();
                assertThat(entityClass).as("Expecting entityClass to be populated").isNotNull();

                for (String binName : binNames) {
                    assertBinIsIndexed(binName, entityClass);
                }
            });
    }

    /**
     * Assert that the given query's statement contains secondary index filter
     *
     * @param query             Query to be performed
     * @param returnEntityClass Class of Query return entity
     */
    protected void assertQueryHasSecIndexFilter(Query query, Class<?> returnEntityClass) {
        String setName = template.getSetName(returnEntityClass);
        String[] binNames = getBinNamesFromTargetClass(returnEntityClass, mappingContext);

        assertThat(queryHasSecIndexFilter(namespace, setName, query, binNames))
            .as(String.format("Expecting the query '%s' statement to have secondary index filter",
                query.getCriteriaObject())).isTrue();
    }

    /**
     * Assert that the given method's query statement contains secondary index filter
     *
     * @param methodName        Query method to be performed
     * @param returnEntityClass Class of Query return entity
     * @param methodParams      Query parameters
     */
    protected void assertQueryHasSecIndexFilter(String methodName, Class<?> returnEntityClass,
                                                Object... methodParams) {
        assertThat(queryHasSecIndexFilter(methodName, returnEntityClass, methodParams))
            .as(String.format("Expecting the query %s statement to have secondary index filter", methodName)).isTrue();
    }

    protected boolean queryHasSecIndexFilter(String methodName, Class<?> returnEntityClass,
                                             Object... methodParams) {
        String setName = template.getSetName(returnEntityClass);
        String[] binNames = getBinNamesFromTargetClass(returnEntityClass, mappingContext);
        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport, methodName, methodParams);

        return queryHasSecIndexFilter(namespace, setName, query, binNames);
    }

    protected boolean queryHasSecIndexFilter(String namespace, String setName, Query query, String[] binNames) {
        Statement statement = queryEngine.getStatementBuilder().build(namespace, setName, query, binNames);
        // Checking that the statement has secondary index filter (which means it will be used)
        return statement.getFilter() != null;
    }

    protected Map<?, ?> pojoToMap(Object pojo) {
        Object result = template.getAerospikeConverter().toWritableValue(pojo, TypeInformation.of(pojo.getClass()));
        if (result instanceof Map<?, ?>) {
            return (Map<?, ?>) result;
        }

        throw new IllegalArgumentException("The result of conversion is not a Map, expecting only a POJO argument");
    }
}
