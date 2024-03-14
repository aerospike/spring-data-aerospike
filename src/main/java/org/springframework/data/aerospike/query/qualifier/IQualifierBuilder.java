package org.springframework.data.aerospike.query.qualifier;

import java.util.Map;

/**
 * An interface for building Qualifier
 */
public interface IQualifierBuilder {

    Map<QualifierField, Object> getMap();

    Qualifier build();
}
