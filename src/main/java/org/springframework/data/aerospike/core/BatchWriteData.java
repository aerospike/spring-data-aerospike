package org.springframework.data.aerospike.core;

import com.aerospike.client.BatchRecord;

/**
 * A class used to encapsulate data required for a single document within a batch write operation.
 * @param document The document (entity) being processed
 * @param batchRecord Corresponding {@link BatchRecord}
 * @param hasVersionProperty A flag indicating if the document has a version property
 * @param <T> Document type
 */
record BatchWriteData<T>(T document, BatchRecord batchRecord, boolean hasVersionProperty) {

}
