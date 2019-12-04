package org.springframework.data.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.aerospike.config.TestConfig;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.repository.query.AerospikeQueryCreator;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.PersonRepository;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(
        classes = TestConfig.class,
        properties = {
                "expirationProperty: 1",
                "setSuffix: service1"
        }
)
public abstract class BaseIntegrationTests {

    private static AtomicLong counter = new AtomicLong();

    @Value("${embedded.aerospike.namespace}")
    protected String namespace;

    @Autowired
    protected AerospikeTemplate template;
    @Autowired
    protected AerospikeClient client;

    private DefaultRepositoryMetadata repositoryMetaData = new DefaultRepositoryMetadata(PersonRepository.class);

    protected String id;

    @Before
    public void setUp() {
        this.id = nextId();
    }

    protected String getNameSpace() {
        return namespace;
    }

    protected static String nextId() {
        return "as-" + counter.incrementAndGet();
    }

    protected void cleanDb() {
        deleteAll(Person.class, Customer.class, SampleClasses.VersionedClass.class);
    }

    @SuppressWarnings("unchecked")
    protected void deleteAll(Class... entityClasses) {
        Arrays.asList(entityClasses).forEach(template::delete);
        Arrays.asList(entityClasses).forEach(this::awaitUntilSetIsEmpty);
    }

    private void awaitUntilSetIsEmpty(Class entityClass) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .until(() -> isEmptySet(client, namespace, entityClass));
    }

    private <T> boolean isEmptySet(IAerospikeClient client, String namespace, Class<T> entityClass) {
        String answer = Info.request(client.getNodes()[0], "sets/" + namespace + "/" + template.getSetName(entityClass));
        return answer.isEmpty()
                || Stream.of(answer.split(";")).allMatch(s -> s.contains("objects=0"));
    }

    protected <T> void createIndexIfNotExists(Class<T> domainType, String indexName, String binName, IndexType indexType) {
        try {
            template.createIndex(domainType, indexName, binName, indexType);
        } catch (InvalidDataAccessResourceUsageException e) {
            // ignore: index already exists
        }
    }

    protected void addNewFieldToSavedDataInAerospike(Key key) {
        Record initial = client.get(new Policy(), key);
        Bin[] bins = Stream.concat(
                initial.bins.entrySet().stream().map(e -> new Bin(e.getKey(), e.getValue())),
                Stream.of(new Bin("notPresent", "cats"))).toArray(Bin[]::new);
        WritePolicy policy = new WritePolicy();
        policy.recordExistsAction = RecordExistsAction.REPLACE;

        client.put(policy, key, bins);

        Record updated = client.get(new Policy(), key);
        assertThat(updated.bins.get("notPresent")).isEqualTo("cats");
    }

    public <T> Query createQueryForMethodWithArgs(String methodName, Object... args) {
        Class[] argTypes = Stream.of(args).map(Object::getClass).toArray(Class[]::new);
        Method method = ReflectionUtils.findMethod(PersonRepository.class, methodName, argTypes);
        PartTree partTree = new PartTree(method.getName(), Person.class);
        AerospikeQueryCreator creator =
                new AerospikeQueryCreator(partTree,
                        new ParametersParameterAccessor(
                                new QueryMethod(method, repositoryMetaData, new SpelAwareProxyProjectionFactory()).getParameters(), args));
        return creator.createQuery();
    }

}
