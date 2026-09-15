package org.springframework.data.aerospike.examples.blocking.indexed.context.repository;

import org.springframework.data.aerospike.examples.blocking.indexed.context.entity.IndexedPersonDocument;
import org.springframework.data.aerospike.repository.AerospikeRepository;

// tag::indexed-context-repository[]
public interface IndexedContextPersonRepository extends AerospikeRepository<IndexedPersonDocument, String> {
}
// end::indexed-context-repository[]
