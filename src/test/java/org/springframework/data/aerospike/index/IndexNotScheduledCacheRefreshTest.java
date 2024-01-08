package org.springframework.data.aerospike.index;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.query.model.IndexedField;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

import static com.aerospike.client.query.IndexType.STRING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Slf4j
@ContextConfiguration
@TestPropertySource(properties = {"spring-data-aerospike.data.server-version-refresh-seconds = 0"})
public class IndexNotScheduledCacheRefreshTest extends BaseBlockingIntegrationTests {

    String setName = "scheduled";
    String indexName = "index1";
    String binName = "testBin";

    @Test
    public void indexesCacheIsNotRefreshedOnSchedule() {
        client.createIndex(null, getNameSpace(), setName, indexName, binName, STRING).waitTillComplete();
        log.debug("Test index {} is created", indexName);
        await()
            .timeout(5, SECONDS)
            .pollDelay(4, SECONDS)
            .untilAsserted(() -> Assertions.assertTrue(true));
        log.debug("Checking indexes");

        assertThat(indexesCache.hasIndexFor(new IndexedField(namespace, setName, binName))).isFalse();
    }
}
