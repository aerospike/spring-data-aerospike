package org.springframework.data.aerospike.repository.query;

import lombok.Getter;
import lombok.Setter;

import java.util.Iterator;

@Getter
@Setter
public class QueryParameters {

    private Object firstParam;
    private Object secondParam;
    private Object queryCriterion;

    public QueryParameters(Iterator<?> parameters) {
//        List<Object> params = new ArrayList<>();
//
//        Object value1 = null;
//        if (parameters.hasNext()) {
//            value1 = parameters.next();
//        }
//        params.add(convertIfNecessary(value1));
//        parameters.forEachRemaining(params::add);
    }
}
