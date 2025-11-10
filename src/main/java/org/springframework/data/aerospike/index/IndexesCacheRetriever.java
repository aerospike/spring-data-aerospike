package org.springframework.data.aerospike.index;

import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.query.model.IndexKey;

import java.util.Map;

/**
 * Provides a method to retrieve cached indexes
 */
public interface IndexesCacheRetriever {

    Map<IndexKey, Index> getIndexesCache();
}
