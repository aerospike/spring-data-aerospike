/*
 * Copyright 2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.query;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class FilterExpressionsBuilder {

    public Expression build(Qualifier[] qualifiers) {
        if (qualifiers != null && qualifiers.length != 0) {
            List<Qualifier> relevantQualifiers = Arrays.stream(qualifiers)
                .filter(Objects::nonNull)
                .filter(this::excludeIrrelevantFilters).toList();

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

    /**
     * The filter allows only qualifiers without sIndexFilter and those with the dualFilterOperation that require both
     * sIndexFilter and FilterExpression. The filter is irrelevant for AND operation (nested qualifiers)
     */
    private boolean excludeIrrelevantFilters(Qualifier qualifier) {
        if (!qualifier.queryAsFilter()) {
            return true;
        } else if (qualifier.queryAsFilter() && FilterOperation.dualFilterOperations.contains(qualifier.getOperation())) {
            qualifier.setQueryAsFilter(false); // clear the flag in case if the same Qualifier is going to be reused
            return true;
        } else {
            qualifier.setQueryAsFilter(false); // clear the flag in case if the same Qualifier is going to be reused
            return false;
        }
    }
}
