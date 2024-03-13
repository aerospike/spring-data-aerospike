package org.springframework.data.aerospike.query.qualifier;

import java.util.Map;

public interface IQualifierBuilder {

    Map<QualifierField, Object> getMap();

    Qualifier build();
}
