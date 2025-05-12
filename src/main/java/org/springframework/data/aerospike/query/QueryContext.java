package org.springframework.data.aerospike.query;

import com.aerospike.client.query.Statement;
import org.springframework.data.aerospike.query.qualifier.Qualifier;

/**
 * This class holds {@link Statement} and {@link Qualifier} used for query building.
 * @param statement
 * @param qualifier
 */
public record QueryContext(Statement statement, Qualifier qualifier) {
}
