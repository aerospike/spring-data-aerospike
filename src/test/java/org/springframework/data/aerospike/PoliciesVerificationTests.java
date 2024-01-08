package org.springframework.data.aerospike;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PoliciesVerificationTests extends BaseBlockingIntegrationTests {

    @Test
    public void sendKeyShouldBeTrueByDefault() {
        assertThat(client.getWritePolicyDefault().sendKey).isTrue();
    }
}
