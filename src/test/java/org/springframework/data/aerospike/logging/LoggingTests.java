package org.springframework.data.aerospike.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.aerospike.client.Value;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.aerospike.query.StatementBuilder;
import org.springframework.data.aerospike.query.cache.IndexesCache;
import org.springframework.data.aerospike.repository.query.AerospikeQueryCreator;
import org.springframework.data.aerospike.repository.query.StubParameterAccessor;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.utility.MemoryAppender;
import org.springframework.data.repository.query.parser.PartTree;

import static org.assertj.core.api.Assertions.assertThat;

public class LoggingTests {

    String LOGGER_NAME = "org.springframework.data.aerospike";
    MemoryAppender memoryAppender;

    @Before
    public void setup() {
        Logger logger = (Logger) LoggerFactory.getLogger(LOGGER_NAME);
        memoryAppender = new MemoryAppender();
        memoryAppender.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        logger.setLevel(Level.DEBUG);
        logger.addAppender(memoryAppender);
        memoryAppender.start();
    }

    @Test
    public void isBinIndexed() {
        IndexesCache indexesCacheMock = Mockito.mock(IndexesCache.class);
        Qualifier qualifier = new Qualifier(new Qualifier.QualifierBuilder()
            .setField("testField")
            .setFilterOperation(FilterOperation.EQ)
            .setValue1(Value.get("testValue1"))
        );
        StatementBuilder statementBuilder = new StatementBuilder(indexesCacheMock);
        statementBuilder.build("TEST", "testSet", null, new Qualifier[] {qualifier});

        assertThat(memoryAppender.countEventsForLogger(LOGGER_NAME)).isEqualTo(1);
        String msg = "Bin TEST.testSet.testField has secondary index: false";
        assertThat(memoryAppender.search(msg, Level.DEBUG).size()).isEqualTo(1);
        assertThat(memoryAppender.contains(msg, Level.INFO)).isFalse();
    }


    @Test
    public void queryCreated() {
        AerospikeMappingContext context = new AerospikeMappingContext();
        PartTree tree = new PartTree("findByFirstName", Person.class);
        AerospikeQueryCreator creator = new AerospikeQueryCreator(tree, new StubParameterAccessor("TestName"), context);
        creator.createQuery();

        assertThat(memoryAppender.countEventsForLogger(LOGGER_NAME)).isEqualTo(1);
        String msg = "Created query: firstName EQ TestName";
        assertThat(memoryAppender.search(msg, Level.DEBUG).size()).isEqualTo(1);
        assertThat(memoryAppender.contains(msg, Level.INFO)).isFalse();
    }
}
