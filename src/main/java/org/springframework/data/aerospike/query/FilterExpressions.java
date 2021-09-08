package org.springframework.data.aerospike.query;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.QueryPolicy;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class FilterExpressions {

    public void setFilterExpressions(QueryPolicy queryPolicy, Qualifier[] qualifiers) {
        queryPolicy.filterExp = buildFilterExpressions(qualifiers);
    }

    private Expression buildFilterExpressions(Qualifier[] qualifiers) {
        if (qualifiers != null && qualifiers.length != 0) {
            List<Qualifier> relevantQualifiers = Arrays.stream(qualifiers)
                    .filter(q -> q != null && !q.queryAsFilter())
                    .collect(Collectors.toList());

            // in case there is more than 1 relevant qualifier -> the default behaviour is AND
            if (relevantQualifiers.size() > 1) {
                Exp[] exps = relevantQualifiers.stream()
                        .map(Qualifier::toFilterExp)
                        .toArray(Exp[]::new);
                Exp finalExp = Exp.and(exps);
                return Exp.build(finalExp);
            } else if (relevantQualifiers.size() == 1) {
                return Exp.build(relevantQualifiers.get(0).toFilterExp());
            }
        }
        return null;
    }
}
