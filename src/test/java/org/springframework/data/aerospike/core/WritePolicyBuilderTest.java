package org.springframework.data.aerospike.core;

import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class WritePolicyBuilderTest {

    private static final GenerationPolicy GENERATION_POLICY = GenerationPolicy.EXPECT_GEN_EQUAL;
    private static final int GENERATION = 10;
    private static final int EXPIRATION = 42;
    private static final RecordExistsAction RECORD_EXISTS_ACTION = RecordExistsAction.REPLACE;
    private static final boolean SEND_KEY = true;

    @Test
    public void shouldFailOnNull() {
        assertThatThrownBy(() -> WritePolicyBuilder.builder(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldFailOnNullCommitLevel() {
        WritePolicy policy = new WritePolicy();
        policy.commitLevel = null;
        assertThatThrownBy(() -> WritePolicyBuilder.builder(policy).build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldFailOnNullRecordExistsAction() {
        assertThatThrownBy(() -> WritePolicyBuilder.builder(new WritePolicy())
                .recordExistsAction(null)
                .build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldFailOnNullGenerationPolicy() {
        assertThatThrownBy(() -> WritePolicyBuilder.builder(new WritePolicy())
                .generationPolicy(null)
                .build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldFailOnNoneGenerationPolicy() {
        assertThatThrownBy(() -> WritePolicyBuilder.builder(new WritePolicy())
                .generation(1)
                .generationPolicy(GenerationPolicy.NONE)
                .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Field 'generationPolicy' must not be 'NONE' when 'generation' is set");
    }

    @Test
    public void shouldBuildWithOverriddenProps() {
        WritePolicy source = new WritePolicy();
        WritePolicy policy = WritePolicyBuilder.builder(source)
                .generationPolicy(GENERATION_POLICY)
                .generation(GENERATION)
                .expiration(EXPIRATION)
                .recordExistsAction(RECORD_EXISTS_ACTION)
                .sendKey(SEND_KEY)
                .build();

        assertThat(policy).isEqualToIgnoringGivenFields(source,
                "generationPolicy", "generation", "expiration", "recordExistsAction", "sendKey");

        assertThat(policy.generationPolicy).isEqualTo(GENERATION_POLICY);
        assertThat(policy.generation).isEqualTo(GENERATION);
        assertThat(policy.expiration).isEqualTo(EXPIRATION);
        assertThat(policy.recordExistsAction).isEqualTo(RECORD_EXISTS_ACTION);
        assertThat(policy.sendKey).isEqualTo(SEND_KEY);
    }

    @Test
    public void shouldNotAffectSource() {
        WritePolicy source = new WritePolicy();
        source.sendKey = true;

        WritePolicyBuilder policyBuilder = WritePolicyBuilder.builder(source);
        source.sendKey = false;

        WritePolicy policy = policyBuilder.build();
        assertThat(source.sendKey).isFalse();
        assertThat(policy.sendKey).isTrue();
    }
}
