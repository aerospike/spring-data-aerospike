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

    /**
     * Retrieves {@link Policy} with a filter expression applied based on the provided query. If the query contains
     * criteria, a filter expression is built using the {@link QueryEngine}. Otherwise, {@code null} is returned.
     *
     * @param client      The Aerospike client instance
     * @param queryEngine The {@link QueryEngine} used to build filter expressions
     * @param query       The {@link Query} object that may contain criteria
     * @return A {@link Policy} with a filter expression if query criteria are present, otherwise {@code null}
     */
    private static Policy getPolicyFilterExp(IAerospikeClient client, QueryEngine queryEngine, Query query) {
        if (isQueryCriteriaNotNull(query)) {
            Policy policy = client.copyReadPolicyDefault();
            Qualifier qualifier = query.getCriteriaObject();
            policy.filterExp = queryEngine.getFilterExpressionsBuilder().build(qualifier);
            return policy;
        }
        return null;
    }

    /**
     * Retrieves {@link Policy} with a filter expression applied if the query contains criteria, otherwise returns a
     * copy of the client's default read policy.
     *
     * @param client      The Aerospike client instance
     * @param queryEngine The {@link QueryEngine} used to build filter expressions
     * @param query       The {@link Query} object that may contain criteria
     * @return A {@link Policy} with a filter expression if query criteria are present, otherwise a default read policy
     */
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
                }
                return policy;
            })
            .onErrorResume(NoTransactionException.class, ignored ->
                Mono.just(policy));
    }

    /**
     * Retrieves {@link BatchPolicy} with a filter expression applied based on the provided query. If the query contains
     * criteria, a filter expression is built using the {@link QueryEngine} from the {@link TemplateContext}. Otherwise,
     * a copy of the client's default batch policy is returned.
     *
     * @param query           The {@link Query} object that may contain criteria
     * @param templateContext The context containing the Aerospike client and query engine
     * @return A {@link BatchPolicy} with a filter expression if query criteria are present, otherwise a default batch
     * policy.
     */
    static BatchPolicy getBatchPolicyFilterExp(Query query, TemplateContext templateContext) {
        if (isQueryCriteriaNotNull(query)) {
            BatchPolicy batchPolicy = templateContext.client.copyBatchPolicyDefault();
            Qualifier qualifier = query.getCriteriaObject();
            batchPolicy.filterExp = templateContext.queryEngine.getFilterExpressionsBuilder().build(qualifier);
            return batchPolicy;
        }
        return templateContext.client.copyBatchPolicyDefault();
    }

    /**
     * Creates {@link WritePolicy} configured for optimistic locking (CAS) based on the document's version.
     * <p>
     * If the document has a version greater than 0, it sets {@link RecordExistsAction#UPDATE_ONLY} and expects the
     * generation to be equal to the document's version. If the document has no version or version is 0, it sets
     * {@link RecordExistsAction#CREATE_ONLY} and expects the generation to be 0 (for new records). This policy is used
     * to prevent concurrent modifications.
     * </p>
     *
     * @param data               The {@link AerospikeWriteData} containing the document's version and expiration
     * @param writePolicyDefault The default {@link WritePolicy} to use as a base
     * @return A {@link WritePolicy} configured for CAS-aware operations
     */
    static WritePolicy expectGenerationCasAwarePolicy(AerospikeWriteData data, WritePolicy writePolicyDefault) {
        RecordExistsAction recordExistsAction = data.getVersion()
            .filter(v -> v > 0L)
            .map(v -> RecordExistsAction.UPDATE_ONLY) // updating existing document with generation,
            // cannot use REPLACE_ONLY due to bin convergence feature restrictions
            .orElse(RecordExistsAction.CREATE_ONLY); // create new document,
        // if exists we should fail with optimistic locking
        return expectGenerationPolicy(data, recordExistsAction, writePolicyDefault);
    }

    /**
     * Creates {@link BatchWritePolicy} configured for optimistic locking (CAS) based on the document's version for
     * batch operations.
     * <p>
     * This method sets the {@link RecordExistsAction} and expected generation based on the document's version. It uses
     * {@link RecordExistsAction#UPDATE_ONLY} for existing documents with a version > 0 and
     * {@link RecordExistsAction#CREATE_ONLY} for new documents.
     * </p>
     *
     * @param data                    The {@link AerospikeWriteData} containing the document's version and expiration
     * @param batchWritePolicyDefault The default {@link BatchWritePolicy} to use as a base
     * @return A {@link BatchWritePolicy} configured for CAS-aware batch operations
     */
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

    /**
     * Creates {@link WritePolicy} that expects the record's generation to be equal to the provided document's version.
     * This is used for optimistic locking. The {@link GenerationPolicy} is set to
     * {@link GenerationPolicy#EXPECT_GEN_EQUAL}.
     *
     * @param data               The {@link AerospikeWriteData} containing the document's version and expiration
     * @param recordExistsAction The {@link RecordExistsAction} to apply
     * @param writePolicyDefault The default {@link WritePolicy} to use as a base
     * @return A {@link WritePolicy} configured to expect a specific generation
     */
    static WritePolicy expectGenerationPolicy(AerospikeWriteData data, RecordExistsAction recordExistsAction,
                                              WritePolicy writePolicyDefault) {
        return WritePolicyBuilder.builder(writePolicyDefault)
            .generationPolicy(GenerationPolicy.EXPECT_GEN_EQUAL)
            .generation(data.getVersion().orElse(0))
            .expiration(data.getExpiration())
            .recordExistsAction(recordExistsAction)
            .build();
    }

    /**
     * Creates {@link BatchWritePolicy} that expects the record's generation to be equal to the provided document's
     * version for batch operations. This is used for optimistic locking in batches. The {@link GenerationPolicy} is set
     * to {@link GenerationPolicy#EXPECT_GEN_EQUAL}.
     *
     * @param data                    The {@link AerospikeWriteData} containing the document's version and expiration
     * @param recordExistsAction      The {@link RecordExistsAction} to apply
     * @param batchWritePolicyDefault The default {@link BatchWritePolicy} to use as a base
     * @return A {@link BatchWritePolicy} configured to expect a specific generation
     */
    static BatchWritePolicy expectGenerationBatchPolicy(AerospikeWriteData data, RecordExistsAction recordExistsAction,
                                                        BatchWritePolicy batchWritePolicyDefault) {
        BatchWritePolicy batchWritePolicy = new BatchWritePolicy(batchWritePolicyDefault);
        batchWritePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
        batchWritePolicy.generation = data.getVersion().orElse(0);
        batchWritePolicy.expiration = data.getExpiration();
        batchWritePolicy.recordExistsAction = recordExistsAction;
        return batchWritePolicy;
    }

    /**
     * Creates {@link WritePolicy} that ignores the record's generation. This policy is used when optimistic locking is
     * not required. The {@link GenerationPolicy} is set to {@link GenerationPolicy#NONE}.
     *
     * @param data               The {@link AerospikeWriteData} containing the document's expiration
     * @param recordExistsAction The {@link RecordExistsAction} to apply
     * @param writePolicyDefault The default {@link WritePolicy} to use as a base
     * @return A {@link WritePolicy} configured to ignore generation
     */
    static WritePolicy ignoreGenerationPolicy(AerospikeWriteData data, RecordExistsAction recordExistsAction,
                                              WritePolicy writePolicyDefault) {
        return WritePolicyBuilder.builder(writePolicyDefault)
            .generationPolicy(GenerationPolicy.NONE)
            .expiration(data.getExpiration())
            .recordExistsAction(recordExistsAction)
            .build();
    }

    /**
     * Creates {@link BatchWritePolicy} that ignores the record's generation for batch operations. This policy is used
     * when optimistic locking is not required for batch writes. The {@link GenerationPolicy} is set to
     * {@link GenerationPolicy#NONE}.
     *
     * @param data                    The {@link AerospikeWriteData} containing the document's expiration
     * @param recordExistsAction      The {@link RecordExistsAction} to apply
     * @param batchWritePolicyDefault The default {@link BatchWritePolicy} to use as a base
     * @return A {@link BatchWritePolicy} configured to ignore generation
     */
    static BatchWritePolicy ignoreGenerationBatchPolicy(AerospikeWriteData data, RecordExistsAction recordExistsAction,
                                                        BatchWritePolicy batchWritePolicyDefault) {
        BatchWritePolicy batchWritePolicy = new BatchWritePolicy(batchWritePolicyDefault);
        batchWritePolicy.generationPolicy = GenerationPolicy.NONE;
        batchWritePolicy.expiration = data.getExpiration();
        batchWritePolicy.recordExistsAction = recordExistsAction;
        return batchWritePolicy;
    }

    /**
     * Creates {@link WritePolicy} that ignores the record's generation, using a provided default write policy as a
     * base. This method is used when only the generation policy needs to be set to {@link GenerationPolicy#NONE}.
     *
     * @param writePolicyDefault The default {@link WritePolicy} to use as a base
     * @return A {@link WritePolicy} configured to ignore generation
     */
    static WritePolicy ignoreGenerationPolicy(WritePolicy writePolicyDefault) {
        return WritePolicyBuilder.builder(writePolicyDefault)
            .generationPolicy(GenerationPolicy.NONE)
            .build();
    }

    /**
     * Creates {@link WritePolicy} that expects the record's generation to be equal to the provided document's version,
     * using a provided default write policy as a base.
     *
     * @param data               The {@link AerospikeWriteData} containing the document's version and expiration
     * @param writePolicyDefault The default {@link WritePolicy} to use as a base.
     * @return A {@link WritePolicy} configured to expect a specific generation.
     */
    static WritePolicy expectGenerationPolicy(AerospikeWriteData data, WritePolicy writePolicyDefault) {
        return WritePolicyBuilder.builder(writePolicyDefault)
            .generationPolicy(GenerationPolicy.EXPECT_GEN_EQUAL)
            .generation(data.getVersion().orElse(0))
            .expiration(data.getExpiration())
            .build();
    }
}
