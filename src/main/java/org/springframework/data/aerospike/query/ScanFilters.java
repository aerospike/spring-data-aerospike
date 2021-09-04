package org.springframework.data.aerospike.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.PredExp;
import org.springframework.data.aerospike.utility.VersionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ScanFilters {

    public void setScanFilters(IAerospikeClient client, QueryPolicy queryPolicy, Qualifier[] qualifiers) {
        if (VersionUtils.isFilterExpressionSupported(client)) {
            queryPolicy.filterExp = buildFilterExp(qualifiers);
        } else {
            PredExp[] predExps = buildPredExp(qualifiers).toArray(new PredExp[0]);
            queryPolicy.predExp = predExps.length > 0 ? predExps : null;
        }
    }

    private Expression buildFilterExp(Qualifier[] qualifiers) {
        System.out.println("FilterExpressionIndication!!!!!!!!!!!!!!!!!!");
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

    /*
        As of Aerospike Database 5.2, Predicate Expressions (PredExps) are obsolete.
        Please use Aerospike Expressions instead.
        The Aerospike Server and Client will support both Predicate Expressions (PredExps) and Aerospike Expressions until November 2021.
        From November 2021 support for Predicate Expressions will be removed from both Aerospike server and clients.
     */
    private List<PredExp> buildPredExp(Qualifier[] qualifiers) {
        System.out.println("PredExpIndication!!!!!!!!!!!!!!!!!!");
        List<PredExp> pes = new ArrayList<>();
        int qCount = 0;
        for (Qualifier q : qualifiers) {
            if (null != q && !q.queryAsFilter()) {
                List<PredExp> tpes = q.toPredExp();
                if (tpes.size() > 0) {
                    pes.addAll(tpes);
                    qCount++;
                }
            }
        }

        if (qCount > 1) pes.add(PredExp.and(qCount));
        return pes;
    }
}
