package org.springframework.data.aerospike.examples.blocking.transactions;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.aerospike.examples.blocking.transactions.repository.BlockingTransactionalMovieRepository;
import org.springframework.data.aerospike.examples.support.ExampleSkippedException;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.stereotype.Component;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;

@Component
public class BlockingTransactionExample {

    private final BlockingTransactionalMovieRepository repository;
    private final BlockingTransactionalMovieService service;
    private final ServerVersionSupport serverVersionSupport;

    public BlockingTransactionExample(BlockingTransactionalMovieRepository repository,
                                      BlockingTransactionalMovieService service,
                                      ServerVersionSupport serverVersionSupport) {
        this.repository = repository;
        this.service = service;
        this.serverVersionSupport = serverVersionSupport;
    }

    public void run() {
        skipUnlessTransactionsSupported();

        try {
            service.saveCommittedMovies();
        } catch (RuntimeException ex) {
            skipIfTransactionFeatureUnavailable(ex);
        }
        require(repository.count() == 2, "Committed blocking transaction should persist both movies");

        try {
            service.rollbackDuplicateInsert();
            throw new IllegalStateException("Duplicate insert should fail and roll back the transaction");
        } catch (DuplicateKeyException expected) {
            // The first insert in the transaction is rolled back when the duplicate insert fails
        }

        require(!repository.existsById("blocking-transaction-duplicate"),
            "Rolled-back blocking transaction should not leave the duplicate record");

        System.out.println("Ran blocking repository and template writes inside Aerospike transactions");
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
