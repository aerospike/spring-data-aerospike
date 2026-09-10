package org.springframework.data.aerospike.examples.reactive.transactions;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.examples.reactive.transactions.entity.ReactiveTransactionalMovieDocument;
import org.springframework.data.aerospike.examples.reactive.transactions.repository.ReactiveTransactionalMovieRepository;
import org.springframework.data.aerospike.examples.support.ExampleSkippedException;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;

@Component
public class ReactiveTransactionExample {

    private final ReactiveTransactionalMovieRepository repository;
    private final ReactiveAerospikeTemplate template;
    private final TransactionalOperator transactionalOperator;
    private final ServerVersionSupport serverVersionSupport;

    public ReactiveTransactionExample(ReactiveTransactionalMovieRepository repository,
                                      ReactiveAerospikeTemplate template,
                                      TransactionalOperator transactionalOperator,
                                      ServerVersionSupport serverVersionSupport) {
        this.repository = repository;
        this.template = template;
        this.transactionalOperator = transactionalOperator;
        this.serverVersionSupport = serverVersionSupport;
    }

    public void run() {
        skipUnlessTransactionsSupported();

        try {
            repository.save(new ReactiveTransactionalMovieDocument(
                    "reactive-transaction-1", "The Conversation", "committed"))
                .then(repository.save(new ReactiveTransactionalMovieDocument(
                    "reactive-transaction-2", "Michael Clayton", "committed")))
                .then()
                .as(transactionalOperator::transactional)
                .block();
        } catch (RuntimeException ex) {
            skipIfTransactionFeatureUnavailable(ex);
        }
        require(repository.count().block() == 2, "Committed reactive transaction should persist both movies");

        try {
            ReactiveTransactionalMovieDocument duplicate =
                new ReactiveTransactionalMovieDocument("reactive-transaction-duplicate", "Duplicate", "rollback");
            template.insert(duplicate)
                .then(template.insert(duplicate))
                .then()
                .as(transactionalOperator::transactional)
                .block();
            throw new IllegalStateException("Duplicate reactive insert should fail and roll back the transaction");
        } catch (DuplicateKeyException expected) {
            // The first insert in the transaction is rolled back when the duplicate insert fails
        }

        require(!Boolean.TRUE.equals(repository.existsById("reactive-transaction-duplicate").block()),
            "Rolled-back reactive transaction should not leave the duplicate record");

        System.out.println("Ran reactive repository and template writes inside Aerospike transactions");
    }

    private void skipUnlessTransactionsSupported() {
        if (!serverVersionSupport.isTxnSupported()) {
            throw new ExampleSkippedException("Aerospike transactions require Server 8.0.0+");
        }
    }

    private void skipIfTransactionFeatureUnavailable(RuntimeException failure) {
        if (isUnsupportedTransactionFeature(failure)) {
            throw new ExampleSkippedException(
                "Aerospike transactions require Server 8.0.0+ and a transaction-enabled namespace");
        }
        throw failure;
    }

    private boolean isUnsupportedTransactionFeature(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && (message.contains("Unsupported Server Feature")
                || message.contains("Failed to add key(s) to transaction monitor"))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
