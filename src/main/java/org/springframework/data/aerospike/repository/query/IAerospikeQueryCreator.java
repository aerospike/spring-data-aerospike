package org.springframework.data.aerospike.repository.query;

import org.springframework.data.aerospike.query.Qualifier;

public interface IAerospikeQueryCreator {

    void validate();

    Qualifier process();
}
