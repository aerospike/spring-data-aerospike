package org.springframework.data.aerospike.repository.query;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.convert.AerospikeCustomConversions;
import org.springframework.data.aerospike.convert.AerospikeTypeAliasAccessor;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.repository.query.parser.PartTree;

import java.util.Collections;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class AerospikeQueryCreatorUnitTests {

    AerospikeMappingContext context;
    AerospikeCustomConversions conversions;
    MappingAerospikeConverter converter;
    AutoCloseable openMocks;

    @BeforeEach
    public void setUp() {
        openMocks = MockitoAnnotations.openMocks(this);
        context = new AerospikeMappingContext();
        conversions = new AerospikeCustomConversions(Collections.emptyList());
        converter = getMappingAerospikeConverter(conversions);
    }

    @AfterEach
    void tearDown() throws Exception {
        openMocks.close();
    }

    @Test
    public void createsQueryCorrectly() {
        PartTree tree = new PartTree("findByFirstName", Person.class);

        AerospikeQueryCreator creator = new AerospikeQueryCreator(
            tree, new StubParameterAccessor("Oliver"), context, conversions, converter);
        Query query = creator.createQuery();
    }

    @Test
    public void createQueryByInList() {
        PartTree tree = new PartTree("findByFirstNameOrFriend", Person.class);

        AerospikeQueryCreator creator = new AerospikeQueryCreator(
            tree, new StubParameterAccessor("Oliver", "Peter"), context, conversions, converter);
        Query query = creator.createQuery();
    }

    private MappingAerospikeConverter getMappingAerospikeConverter(AerospikeCustomConversions conversions) {
        MappingAerospikeConverter converter = new MappingAerospikeConverter(new AerospikeMappingContext(),
            conversions, new AerospikeTypeAliasAccessor(), AerospikeDataSettings.builder().build());
        converter.afterPropertiesSet();
        return converter;
    }
}
