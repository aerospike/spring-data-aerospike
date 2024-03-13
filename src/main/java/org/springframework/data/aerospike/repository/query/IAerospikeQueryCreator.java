package org.springframework.data.aerospike.repository.query;

import org.springframework.data.aerospike.query.qualifier.Qualifier;

public interface IAerospikeQueryCreator {

    void validate();

    Qualifier process();
}
