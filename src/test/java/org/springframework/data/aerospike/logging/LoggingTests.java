package org.springframework.data.aerospike.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.aerospike.client.Value;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.StatementBuilder;
import org.springframework.data.aerospike.query.cache.IndexesCache;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.aerospike.util.MemoryAppender;
import org.springframework.data.aerospike.util.QueryUtils;

import static org.assertj.core.api.Assertions.assertThat;

public class LoggingTests {

    static String LOGGER_NAME = "org.springframework.data.aerospike";
    static MemoryAppender memoryAppender;

    @BeforeAll
    public static void setup() {
        Logger logger = (Logger) LoggerFactory.getLogger(LOGGER_NAME);
        memoryAppender = new MemoryAppender();
        memoryAppender.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        logger.setLevel(Level.DEBUG);
        logger.addAppender(memoryAppender);
        memoryAppender.start();
    }

    @BeforeEach
    public void beforeEach() {
        memoryAppender.reset();
    }

    @Test
    void binIsIndexed() {
        IndexesCache indexesCacheMock = Mockito.mock(IndexesCache.class);
        Qualifier qualifier = Qualifier.builder()
            .setPath("testField")
            .setFilterOperation(FilterOperation.EQ)
            .setValue(Value.get("testValue1"))
            .build();

        StatementBuilder statementBuilder = new StatementBuilder(indexesCacheMock);
        statementBuilder.build("TEST", "testSet", new Query(qualifier));

        // 3 events: Created query, Bin has secondary index, Secondary index filter is not set
        assertThat(memoryAppender.countEventsForLogger(LOGGER_NAME)).isEqualTo(3);
        String msg = "Bin TEST.testSet.testField has secondary index: false";
        assertThat(memoryAppender.search(msg, Level.DEBUG).size()).isEqualTo(1);
        assertThat(memoryAppender.contains(msg, Level.INFO)).isFalse();
    }

    @Test
    void queryIsCreated_RepositoryQuery() {
        StatementBuilder statementBuilder = new StatementBuilder(Mockito.mock(IndexesCache.class));
        ServerVersionSupport serverVersionSupport = Mockito.mock(ServerVersionSupport.class);

        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport, "findByFirstName", "TestName");
        statementBuilder.build("TEST", "Person", query, null);

        assertThat(memoryAppender.countEventsForLogger(LOGGER_NAME)).isPositive();
        String msg = "Created query: path = firstName, operation = EQ, value = TestName";
        assertThat(memoryAppender.search(msg, Level.DEBUG).size()).isEqualTo(1);
        assertThat(memoryAppender.contains(msg, Level.INFO)).isFalse();
    }

    @Test
    void queryIsCreated_CustomQuery() {
        StatementBuilder statementBuilder = new StatementBuilder(Mockito.mock(IndexesCache.class));
        Query query = new Query(Qualifier.builder()
            .setPath("firstName")
            .setFilterOperation(FilterOperation.EQ)
            .setValue(Value.get("TestName"))
            .build());
        statementBuilder.build("TEST", "Person", query, null);

        assertThat(memoryAppender.countEventsForLogger(LOGGER_NAME)).isPositive();
        String msg = "Created query: path = firstName, operation = EQ, value = TestName";
        assertThat(memoryAppender.search(msg, Level.DEBUG).size()).isEqualTo(1);
        assertThat(memoryAppender.contains(msg, Level.INFO)).isFalse();
    }
}
