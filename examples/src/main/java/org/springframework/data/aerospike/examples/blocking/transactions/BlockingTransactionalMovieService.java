package org.springframework.data.aerospike.examples.blocking.transactions;

import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.examples.blocking.transactions.entity.BlockingTransactionalMovieDocument;
import org.springframework.data.aerospike.examples.blocking.transactions.repository.BlockingTransactionalMovieRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class BlockingTransactionalMovieService {

    private final BlockingTransactionalMovieRepository repository;
    private final AerospikeTemplate template;

    public BlockingTransactionalMovieService(BlockingTransactionalMovieRepository repository,
                                             AerospikeTemplate template) {
        this.repository = repository;
        this.template = template;
    }

    @Transactional(transactionManager = "aerospikeTransactionManager")
    public void saveCommittedMovies() {
        repository.save(new BlockingTransactionalMovieDocument(
            "blocking-transaction-1", "The Conversation", "committed"));
        repository.save(new BlockingTransactionalMovieDocument(
            "blocking-transaction-2", "Michael Clayton", "committed"));
    }

    @Transactional(transactionManager = "aerospikeTransactionManager")
    public void rollbackDuplicateInsert() {
        BlockingTransactionalMovieDocument duplicate =
            new BlockingTransactionalMovieDocument("blocking-transaction-duplicate", "Duplicate", "rollback");

        template.insert(duplicate);
        template.insert(duplicate);
    }
}
