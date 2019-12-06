package org.springframework.data.aerospike;

import com.playtika.test.aerospike.AerospikeTestOperations;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.aerospike.config.TestConfig;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.repository.query.AerospikeQueryCreator;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.sample.PersonRepository;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(
        classes = TestConfig.class,
        properties = {
                "expirationProperty: 1",
                "setSuffix: service1"
        }
)
public abstract class BaseIntegrationTests {

    private static final AtomicLong counter = new AtomicLong();

    @Value("${embedded.aerospike.namespace}")
    protected String namespace;

    private DefaultRepositoryMetadata repositoryMetaData = new DefaultRepositoryMetadata(PersonRepository.class);

    protected String id;

    @Autowired
    protected AerospikeTestOperations aerospikeTestOperations;

    @Autowired
    protected BlockingAerospikeTestOperations blockingAerospikeTestOperations;

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
