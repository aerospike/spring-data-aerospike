package org.springframework.data.aerospike;

import org.junit.jupiter.api.Test;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.query.cache.IndexRefresher.INDEX_CACHE_REFRESH_SECONDS;

@TestPropertySource(properties = {INDEX_CACHE_REFRESH_SECONDS + " = 0", "createIndexesOnStartup = false"})
// this test class does not require secondary indexes created on startup
public class PoliciesVerificationTests extends BaseBlockingIntegrationTests {

    @Test
    public void sendKeyShouldBeTrueByDefault() {
        assertThat(client.getWritePolicyDefault().sendKey).isTrue();
    }
}
