package org.springframework.data.aerospike.repository.query;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.aerospike.convert.AerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class AerospikeQueryCreatorUnitTests {

    MappingContext<?, AerospikePersistentProperty> context;
    @Mock
    AerospikeConverter converter;
    AutoCloseable openMocks;

    @BeforeEach
    public void setUp() {
        openMocks = MockitoAnnotations.openMocks(this);
        context = new AerospikeMappingContext();
    }

    @AfterEach
    void tearDown() throws Exception {
        openMocks.close();
    }

    @Test
    public void createsQueryCorrectly() {
        PartTree tree = new PartTree("findByFirstName", Person.class);

        AerospikeQueryCreator creator = new AerospikeQueryCreator(tree, new StubParameterAccessor(converter,
            "Oliver"), context);
        Query query = creator.createQuery();
    }

    @Test
    public void createQueryByInList() {
        PartTree tree = new PartTree("findByFirstNameOrFriend", Person.class);

        AerospikeQueryCreator creator = new AerospikeQueryCreator(tree, new StubParameterAccessor(converter,
            new String[]{"Oliver", "Peter"}), context);
        Query query = creator.createQuery();
    }
}
