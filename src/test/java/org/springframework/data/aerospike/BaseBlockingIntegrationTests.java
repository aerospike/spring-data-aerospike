package org.springframework.data.aerospike;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.query.Statement;
import org.junit.jupiter.api.TestInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.data.aerospike.config.BlockingTestConfig;
import org.springframework.data.aerospike.config.CommonTestConfig;
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
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.config.IndexedBinsAnnotationsProcessor.getBinNames;
import static org.springframework.data.aerospike.config.IndexedBinsAnnotationsProcessor.getEntityClass;
import static org.springframework.data.aerospike.config.IndexedBinsAnnotationsProcessor.hasAssertBinsAreIndexedAnnotation;
import static org.springframework.data.aerospike.config.IndexedBinsAnnotationsProcessor.hasNoindexAnnotation;
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
        Optional<Method> testMethodOptional = testInfo.getTestMethod();
        if (testMethodOptional.isPresent()) {
            Method testMethod = testMethodOptional.get();
            if (!hasNoindexAnnotation(testMethod)) {
                assertThat(hasAssertBinsAreIndexedAnnotation(testMethod))
                    .as("Expecting the test method to have @AssertBinsAreIndexed annotation").isTrue();
                String[] binNames = getBinNames(testMethod);
                Class<?> entityClass = getEntityClass(testMethod);
                assertThat(binNames).as("Expecting bin names to be populated").isNotNull();
                assertThat(binNames).as("Expecting bin names ").isNotEmpty();
                assertThat(entityClass).as("Expecting entityClass to be populated").isNotNull();

                for (String binName : binNames) {
                    assertBinIsIndexed(binName, entityClass);
                }
            }
        }
    }

    /**
     * Assert that the given query statement contains secondary index filter
     *
     * @param methodName        Query method to be performed
     * @param returnEntityClass Class of Query return entity
     * @param methodParams      Query parameters
     */
    protected void assertStmtHasSecIndexFilter(String methodName, Class<?> returnEntityClass,
                                               Object... methodParams) {
        assertThat(stmtHasSecIndexFilter(methodName, returnEntityClass, methodParams))
            .as(String.format("Expecting the query %s statement to have secondary index filter", methodName)).isTrue();
    }

    protected boolean stmtHasSecIndexFilter(String methodName, Class<?> returnTypeClass,
                                            Object... methodParams) {
        String setName = template.getSetName(returnTypeClass);
        String[] binNames = getBinNamesFromTargetClass(returnTypeClass, mappingContext);
        Query query = QueryUtils.createQueryForMethodWithArgs(methodName, methodParams);

        Statement statement = queryEngine.getStatementBuilder().build(namespace, setName, query, binNames);
        // Checking that the statement has secondary index filter (which means it will be used)
        return statement.getFilter() != null;
    }
}
