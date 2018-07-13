package org.springframework.data.aerospike.core;

import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import org.springframework.util.Assert;

import static com.aerospike.client.policy.GenerationPolicy.NONE;

public class WritePolicyBuilder {

    private WritePolicy policy;

    private WritePolicyBuilder(WritePolicy policy) {
        this.policy = policy;
    }

    public static WritePolicyBuilder builder(WritePolicy policy) {
        Assert.notNull(policy, "Policy must not be null!");
        return new WritePolicyBuilder(new WritePolicy(policy));
    }

    public WritePolicyBuilder expiration(int expiration) {
        policy.expiration = expiration;
        return this;
    }

    public WritePolicyBuilder sendKey(boolean sendKey) {
        policy.sendKey = sendKey;
        return this;
    }

    public WritePolicyBuilder recordExistsAction(RecordExistsAction recordExistsAction) {
        policy.recordExistsAction = recordExistsAction;
        return this;
    }

    public WritePolicyBuilder generationPolicy(GenerationPolicy generationPolicy) {
        policy.generationPolicy = generationPolicy;
        return this;
    }

    public WritePolicyBuilder generation(int generation) {
        policy.generation = generation;
        return this;
    }

    public WritePolicy build() {
        validate();
        return new WritePolicy(policy);
    }

    private void validate() {
        Assert.notNull(policy.commitLevel, "Field 'commitLevel' must not be null");
        Assert.notNull(policy.recordExistsAction, "Field 'recordExistsAction' must not be null");
        Assert.notNull(policy.generationPolicy, "Field 'generationPolicy' must not be null");
        Assert.state(!(policy.generation > 0 && policy.generationPolicy == NONE),
                "Field 'generationPolicy' must not be 'NONE' when 'generation' is set");
    }
}
