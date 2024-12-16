package org.springframework.data.aerospike.repository.query;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.convert.AerospikeCustomConversions;
import org.springframework.data.aerospike.convert.AerospikeTypeAliasAccessor;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.repository.query.parser.PartTree;

import java.util.Collections;
import java.util.List;

import static org.springframework.data.aerospike.convert.AerospikeConverter.CLASS_KEY_DEFAULT;


/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class AerospikeQueryCreatorUnitTests {

    AerospikeMappingContext context;
    AerospikeCustomConversions conversions;
    MappingAerospikeConverter converter;
    AutoCloseable openMocks;
    ServerVersionSupport serverVersionSupport;

    @BeforeEach
    public void setUp() {
        openMocks = MockitoAnnotations.openMocks(this);
        context = new AerospikeMappingContext();
        conversions = new AerospikeCustomConversions(Collections.emptyList());
        converter = getMappingAerospikeConverter(conversions);
        serverVersionSupport = Mockito.mock(ServerVersionSupport.class);
    }

    @AfterEach
    void tearDown() throws Exception {
        openMocks.close();
    }

    @Test
    public void createsQueryCorrectly() {
        PartTree tree = new PartTree("findByFirstName", Person.class);

        AerospikeQueryCreator creator = new AerospikeQueryCreator(
            tree, new StubParameterAccessor("Oliver"), context, converter, serverVersionSupport);
        creator.createQuery();
    }

    @Test
    public void createQueryByInList() {
        PartTree tree1 = new PartTree("findByFirstNameInOrFriend", Person.class);
        AerospikeQueryCreator creator1 = new AerospikeQueryCreator(
            tree1, new StubParameterAccessor(
            QueryParam.of(List.of("Oliver", "Peter")),
            QueryParam.of(new Person("id", "firstName"))
        ), context, converter, serverVersionSupport);
        creator1.createQuery();
    }

    private MappingAerospikeConverter getMappingAerospikeConverter(AerospikeCustomConversions conversions) {
        MappingAerospikeConverter converter = new MappingAerospikeConverter(new AerospikeMappingContext(),
            conversions, new AerospikeTypeAliasAccessor(CLASS_KEY_DEFAULT), new AerospikeDataSettings(null));
        converter.afterPropertiesSet();
        return converter;
    }
}
