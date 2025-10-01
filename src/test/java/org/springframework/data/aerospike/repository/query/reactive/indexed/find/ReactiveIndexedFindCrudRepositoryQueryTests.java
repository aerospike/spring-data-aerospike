package org.springframework.data.aerospike.repository.query.reactive.indexed.find;

import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.repository.query.reactive.indexed.ReactiveIndexedPersonRepositoryQueryTests;

import java.util.List;

/**
 * Tests for the CrudRepository queries API.
 */
public class ReactiveIndexedFindCrudRepositoryQueryTests extends ReactiveIndexedPersonRepositoryQueryTests {

    @Override
    protected List<Index> newIndexes() {
        return List.of();
    }

}
