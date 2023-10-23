package org.springframework.data.aerospike;

import com.playtika.testcontainer.aerospike.AerospikeTestOperations;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.aerospike.utility.AdditionalAerospikeTestOperations;
import org.springframework.data.aerospike.utility.AerospikeUniqueId;

public abstract class BaseIntegrationTests {

    public static final String DEFAULT_SET_NAME = "aerospike";
    public static final String OVERRIDE_SET_NAME = "testSet1";

    @Value("${embedded.aerospike.namespace}")
    protected String namespace;

    protected String id;

    @Autowired
    protected AerospikeTestOperations aerospikeTestOperations;

    @Autowired
    protected AdditionalAerospikeTestOperations additionalAerospikeTestOperations;

    protected static String nextId() {
        return AerospikeUniqueId.nextId();
    }

    @BeforeEach
    public void setUp() {
        this.id = nextId();
    }

    protected String getNameSpace() {
        return namespace;
    }
}
