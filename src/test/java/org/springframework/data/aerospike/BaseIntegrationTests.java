package org.springframework.data.aerospike;

import com.playtika.testcontainer.aerospike.AerospikeTestOperations;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.util.AdditionalAerospikeTestOperations;
import org.springframework.data.aerospike.util.AerospikeUniqueId;

import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class BaseIntegrationTests {

    public static final String DEFAULT_SET_NAME = "aerospike";
    public static final String OVERRIDE_SET_NAME = "testSet1";
    public static final String DIFFERENT_SET_NAME = "different-set";
    public static final String CACHE_WITH_TTL = "CACHE-WITH-TTL";
    public static final String DIFFERENT_EXISTING_CACHE = "DIFFERENT-EXISTING-CACHE";
    protected static final int MILLIS_TO_NANO = 1_000_000;

    @Value("${spring.data.aerospike.namespace}")
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

    /**
     * Returns bin name chosen for secondary index Filter based either on cardinality of indexes
     * or on sequential order of supplied indexes (returns the first one if cardinality is not the same for all).
     *
     * @param indexes One or more indexes to extract cardinality and bin names from
     * @return Bin name if indexes are not empty, otherwise null
     */
    public static String getBinNameForFilter(Index... indexes) {
        if (indexes  == null || indexes.length == 0) return null;
        Set<Integer> distinctCardinalityValues = Stream.of(indexes)
            .map(Index::getBinValuesRatio)
            .collect(Collectors.toSet());
        if (distinctCardinalityValues.size() > 1) {
            return Stream.of(indexes)
                .min(Comparator.comparing(Index::getBinValuesRatio))
                .get()
                .getBin();
        }
        return indexes[0].getBin();
    }
}
