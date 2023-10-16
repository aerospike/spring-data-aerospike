package org.springframework.data.aerospike.query;

import java.util.Map;

public interface QualifierMapBuilder {

    Map<String, Object> buildMap();
}
