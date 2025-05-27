package org.springframework.data.aerospike.core;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import org.springframework.data.aerospike.convert.AerospikeWriteData;
import org.springframework.data.aerospike.query.QueryEngine;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.transaction.reactive.AerospikeReactiveTransactionResourceHolder;
import org.springframework.data.aerospike.transaction.sync.AerospikeTransactionResourceHolder;
import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.reactive.TransactionContextManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import reactor.core.publisher.Mono;

import static org.springframework.data.aerospike.query.QualifierUtils.isQueryCriteriaNotNull;

/**
 * A utility class providing methods for initializing and processing {@link Policy} instances.
 */
public final class PolicyUtils {

    private PolicyUtils() {
        throw new UnsupportedOperationException("Utility class PolicyUtils cannot be instantiated");
    }

    /**
     * Enrich given Policy with transaction id if transaction is active
     *
     * @param client IAerospikeClient
     * @param policy Policy instance, typically not default policy to avoid saving transaction id to defaults
     * @return Policy with filled {@link Policy#txn} if transaction is active
     */
    public static Policy enrichPolicyWithTransaction(IAerospikeClient client, Policy policy) {
        if (TransactionSynchronizationManager.hasResource(client)) {
            AerospikeTransactionResourceHolder resourceHolder =
                (AerospikeTransactionResourceHolder) TransactionSynchronizationManager.getResource(client);
            if (resourceHolder != null) policy.txn = resourceHolder.getTransaction();
            return policy;
        }
        return policy;
    }

    private static Policy getPolicyFilterExp(IAerospikeClient client, QueryEngine queryEngine, Query query) {
        if (isQueryCriteriaNotNull(query)) {
            Policy policy = client.copyReadPolicyDefault();
            Qualifier qualifier = query.getCriteriaObject();
            policy.filterExp = queryEngine.getFilterExpressionsBuilder().build(qualifier);
            return policy;
        }
        return null;
    }

    static Policy getPolicyFilterExpOrDefault(IAerospikeClient client, QueryEngine queryEngine, Query query) {
        Policy policy = getPolicyFilterExp(client, queryEngine, query);
        return policy != null ? policy : client.copyReadPolicyDefault();
    }

    /**
     * Enrich given Policy with transaction id if transaction is active
     *
     * @param reactorClient IAerospikeReactorClient
     * @param policy        Policy instance, typically not default policy to avoid saving transaction id to defaults
     * @return Mono&lt;Policy&gt; with filled {@link Policy#txn} if transaction is active
     */
    static Mono<Policy> enrichPolicyWithTransaction(IAerospikeReactorClient reactorClient, Policy policy) {
        return TransactionContextManager.currentContext()
            .map(ctx -> {
                AerospikeReactiveTransactionResourceHolder resourceHolder =
                    (AerospikeReactiveTransactionResourceHolder) ctx.getResources().get(reactorClient);
                if (resourceHolder != null) {
                    policy.txn = resourceHolder.getTransaction();
                    return policy;
                }
                return policy;
            })
            .onErrorResume(NoTransactionException.class, ignored ->
                Mono.just(policy));
    }

    static BatchPolicy getBatchPolicyFilterExp(Query query, TemplateContext templateContext) {
        if (isQueryCriteriaNotNull(query)) {
            BatchPolicy batchPolicy = templateContext.client.copyBatchPolicyDefault();
            Qualifier qualifier = query.getCriteriaObject();
            batchPolicy.filterExp = templateContext.queryEngine.getFilterExpressionsBuilder().build(qualifier);
            return batchPolicy;
        }
        return templateContext.client.copyBatchPolicyDefault();
    }

    static WritePolicy expectGenerationCasAwarePolicy(AerospikeWriteData data, WritePolicy writePolicyDefault) {
        RecordExistsAction recordExistsAction = data.getVersion()
            .filter(v -> v > 0L)
            .map(v -> RecordExistsAction.UPDATE_ONLY) // updating existing document with generation,
            // cannot use REPLACE_ONLY due to bin convergence feature restrictions
            .orElse(RecordExistsAction.CREATE_ONLY); // create new document,
        // if exists we should fail with optimistic locking
        return expectGenerationPolicy(data, recordExistsAction, writePolicyDefault);
    }

    static BatchWritePolicy expectGenerationCasAwareBatchPolicy(AerospikeWriteData data,
                                                                BatchWritePolicy batchWritePolicyDefault) {
        RecordExistsAction recordExistsAction = data.getVersion()
            .filter(v -> v > 0L)
            .map(v -> RecordExistsAction.UPDATE_ONLY) // updating existing document with generation,
            // cannot use REPLACE_ONLY due to bin convergence feature restrictions
            .orElse(RecordExistsAction.CREATE_ONLY); // create new document,
        // if exists we should fail with optimistic locking
        return expectGenerationBatchPolicy(data, recordExistsAction, batchWritePolicyDefault);
    }

    static WritePolicy expectGenerationPolicy(AerospikeWriteData data, RecordExistsAction recordExistsAction,
                                              WritePolicy writePolicyDefault) {
        return WritePolicyBuilder.builder(writePolicyDefault)
            .generationPolicy(GenerationPolicy.EXPECT_GEN_EQUAL)
            .generation(data.getVersion().orElse(0))
            .expiration(data.getExpiration())
            .recordExistsAction(recordExistsAction)
            .build();
    }

    static BatchWritePolicy expectGenerationBatchPolicy(AerospikeWriteData data, RecordExistsAction recordExistsAction,
                                                        BatchWritePolicy batchWritePolicyDefault) {
        BatchWritePolicy batchWritePolicy = new BatchWritePolicy(batchWritePolicyDefault);
        batchWritePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
        batchWritePolicy.generation = data.getVersion().orElse(0);
        batchWritePolicy.expiration = data.getExpiration();
        batchWritePolicy.recordExistsAction = recordExistsAction;
        return batchWritePolicy;
    }

    static WritePolicy ignoreGenerationPolicy(AerospikeWriteData data, RecordExistsAction recordExistsAction,
                                              WritePolicy writePolicyDefault) {
        return WritePolicyBuilder.builder(writePolicyDefault)
            .generationPolicy(GenerationPolicy.NONE)
            .expiration(data.getExpiration())
            .recordExistsAction(recordExistsAction)
            .build();
    }

    static BatchWritePolicy ignoreGenerationBatchPolicy(AerospikeWriteData data, RecordExistsAction recordExistsAction,
                                                        BatchWritePolicy batchWritePolicyDefault) {
        BatchWritePolicy batchWritePolicy = new BatchWritePolicy(batchWritePolicyDefault);
        batchWritePolicy.generationPolicy = GenerationPolicy.NONE;
        batchWritePolicy.expiration = data.getExpiration();
        batchWritePolicy.recordExistsAction = recordExistsAction;
        return batchWritePolicy;
    }

    static WritePolicy ignoreGenerationPolicy(WritePolicy writePolicyDefault) {
        return WritePolicyBuilder.builder(writePolicyDefault)
            .generationPolicy(GenerationPolicy.NONE)
            .build();
    }

    static WritePolicy expectGenerationPolicy(AerospikeWriteData data, WritePolicy writePolicyDefault) {
        return WritePolicyBuilder.builder(writePolicyDefault)
            .generationPolicy(GenerationPolicy.EXPECT_GEN_EQUAL)
            .generation(data.getVersion().orElse(0))
            .expiration(data.getExpiration())
            .build();
    }
}
