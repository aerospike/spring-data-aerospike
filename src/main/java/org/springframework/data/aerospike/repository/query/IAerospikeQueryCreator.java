package org.springframework.data.aerospike.repository.query;

import org.springframework.data.aerospike.query.qualifier.Qualifier;

/**
 * An interface that provides methods to create a query
 */
public interface IAerospikeQueryCreator {

    void validate();

    Qualifier process();
}
