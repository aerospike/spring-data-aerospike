package org.springframework.data.aerospike.transaction.sync;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Txn;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.sample.SampleClasses;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.transaction.TransactionDefinition.PROPAGATION_MANDATORY;
import static org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRED;
import static org.springframework.transaction.TransactionDefinition.PROPAGATION_SUPPORTS;

public class AerospikeTransactionTestUtils {

    private final IAerospikeClient client;
    private final AerospikeTemplate template;

    public AerospikeTransactionTestUtils(IAerospikeClient client, AerospikeTemplate template) {
        this.client = client;
        this.template = template;
    }

    protected void verifyTwoWritesEachInOngoingTransactionWithPropagation(int propagationType,
                                                                          int numberOfCommitCalls,
                                                                          int numberOfSuspendCalls) {
        verifyTwoWritesEachInOngoingTransactionWithPropagation(propagationType, numberOfCommitCalls,
            numberOfSuspendCalls, false);
    }

    protected void verifyTwoWritesEachInOngoingTransactionWithPropagation(int propagationType,
                                                                          int numberOfCommitCalls,
                                                                          int numberOfSuspendCalls,
                                                                          boolean isPropagationNested) {
        // in the regular flow binding a resource is done within doBegin() in AerospikeTransactionManager
        // binding resource holder manually here to make getTransaction() recognize an ongoing transaction
        // and not to start a new transaction automatically in order to be able to start it manually later
        AerospikeTransactionResourceHolder resourceHolder = new AerospikeTransactionResourceHolder(client);
        TransactionSynchronizationManager.bindResource(client, resourceHolder);

        AerospikeTransactionManager trackedTransactionManager = spy(new AerospikeTransactionManager(client));
        if (isPropagationNested) trackedTransactionManager.setNestedTransactionAllowed(true);
        TransactionTemplate trackedTransactionTemplate = spy(new TransactionTemplate(trackedTransactionManager));

        // initialize a new transaction with the existing resource holder if it is already bound
        TransactionStatus trackedTransactionStatus = spy(
            trackedTransactionManager.getTransaction(new DefaultTransactionDefinition()));
        resourceHolder.setSynchronizedWithTransaction(true);

        trackedTransactionTemplate.setPropagationBehavior(propagationType);
        // execute() will call getTransaction() with the existing resource holder
        // and handleExistingTransaction() with behaviour depending on the given propagation
        trackedTransactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                template.insert(new SampleClasses.DocumentWithPrimitiveIntId(100));
                template.save(new SampleClasses.DocumentWithPrimitiveIntId(200));
            }
        });

        // execute() will call getTransaction() with the existing resource holder
        // and handleExistingTransaction() with behaviour depending on the given propagation
        trackedTransactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                template.insert(new SampleClasses.DocumentWithPrimitiveIntId(300));
                template.save(new SampleClasses.DocumentWithPrimitiveIntId(400));
            }
        });

        List<Integer> propagationToUseExistingTransaction = List.of(PROPAGATION_REQUIRED, PROPAGATION_SUPPORTS,
            PROPAGATION_MANDATORY);

        if (propagationToUseExistingTransaction.contains(propagationType)) {
            // changing the status manually here to simulate a new transaction
            // because otherwise with an ongoing transaction (isNextTransaction() == false) and the given propagation
            // doBegin() and doCommit() are not called automatically waiting to participate in the ongoing transaction
            doReturn(true).when(trackedTransactionStatus).isNewTransaction();
        }
        trackedTransactionManager.commit(trackedTransactionStatus);

        verify(trackedTransactionManager, times(numberOfCommitCalls)).doCommit(any());
        verify(trackedTransactionManager, times(numberOfSuspendCalls)).doSuspend(any());

        if (!propagationToUseExistingTransaction.contains(propagationType)) {
            // otherwise cleanup happens automatically
            TransactionSynchronizationManager.unbindResource(client); // cleanup
            TransactionSynchronizationManager.clear();
            resourceHolder.clear();
        }
    }

    protected static Txn getTransaction(IAerospikeClient client) {
        Txn tran = null;
        if (TransactionSynchronizationManager.hasResource(client)) {
            AerospikeTransactionResourceHolder resourceHolder =
                (AerospikeTransactionResourceHolder) TransactionSynchronizationManager.getResource(client);
            tran = resourceHolder.getTransaction();
        }
        return tran;
    }

    protected static Txn getTransaction2(IAerospikeClient client) {
        Txn tran = null;
        if (TransactionSynchronizationManager.hasResource(client)) {
            AerospikeTransactionResourceHolder resourceHolder =
                (AerospikeTransactionResourceHolder) TransactionSynchronizationManager.getResource(client);
            tran = resourceHolder.getTransaction();
        }
        return tran;
    }

    protected static Txn callGetTransaction(IAerospikeClient client) {
        return getTransaction(client);
    }

    @FunctionalInterface
    protected interface TransactionAction<T> {

        void perform(T argument, TransactionStatus status);
    }

    protected <T> void performInTxAndVerifyCommit(PlatformTransactionManager txManager, TransactionTemplate txTemplate,
                                               T documents, TransactionAction<T> action) {
        txTemplate.executeWithoutResult(status -> action.perform(documents, status));

        // verify that commit() was called
        verify(txManager).commit(any());

        // resource holder must be already released
        assertThat(TransactionSynchronizationManager.getResource(client)).isNull();
    }
}
