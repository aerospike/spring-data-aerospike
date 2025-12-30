package org.springframework.data.aerospike;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PoliciesVerificationTests extends BaseBlockingIntegrationTests {

    @Test
    public void sendKeyShouldBeTrueByDefault() {
        assertThat(client.getWritePolicyDefault().sendKey).isTrue();
        assertThat(client.getReadPolicyDefault().sendKey).isTrue();
        assertThat(client.getBatchPolicyDefault().sendKey).isTrue();
        assertThat(client.getBatchWritePolicyDefault().sendKey).isTrue();
        assertThat(client.getBatchDeletePolicyDefault().sendKey).isTrue();
        assertThat(client.getQueryPolicyDefault().sendKey).isTrue();
        assertThat(client.getScanPolicyDefault().sendKey).isTrue();
    }
}
