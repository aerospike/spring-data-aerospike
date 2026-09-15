package org.springframework.data.aerospike.examples.blocking.indexed.context;

import org.springframework.data.aerospike.examples.blocking.indexed.context.entity.IndexedAddress;
import org.springframework.data.aerospike.examples.blocking.indexed.context.entity.IndexedFriend;
import org.springframework.data.aerospike.examples.blocking.indexed.context.entity.IndexedPersonDocument;
import org.springframework.data.aerospike.examples.blocking.indexed.context.repository.IndexedContextPersonRepository;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;

// Demonstrates startup index creation for a nested Aerospike context path.
public class IndexedContextExample {

    private final IndexedContextPersonRepository repository;

    public IndexedContextExample(IndexedContextPersonRepository repository) {
        this.repository = repository;
    }

    public void run() {
        // tag::indexed-context-usage[]
        IndexedAddress address = new IndexedAddress("Main Street", 14, "12345", "Portland");
        IndexedFriend friend = new IndexedFriend("Carter", address);

        IndexedPersonDocument person = repository.save(new IndexedPersonDocument("indexed-context-1", friend));
        // end::indexed-context-usage[]

        require(person.getFriend().getAddress().getCity().equals("Portland"),
            "Expected nested indexed context document to round-trip");

        System.out.println("Saved a document with a nested @Indexed context");
    }
}
