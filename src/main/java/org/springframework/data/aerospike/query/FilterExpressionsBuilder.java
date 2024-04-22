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
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;

import static org.springframework.data.aerospike.query.FilterOperation.dualFilterOperations;
import static org.springframework.data.aerospike.query.QualifierUtils.queryCriteriaIsNotNull;

public class FilterExpressionsBuilder {

    public Expression build(Query query) {
        Qualifier qualifier = queryCriteriaIsNotNull(query) ? query.getCriteriaObject() : null;
        if (qualifier != null && requiresFilterExp(qualifier)) {
            return Exp.build(qualifier.getFilterExp());
        }
        return null;
    }

    /**
     * FilterExp is built only for a qualifier without sIndexFilter or for dualFilterOperation that requires both
     * sIndexFilter and FilterExpression. The filter is irrelevant for AND operation (nested qualifiers)
     */
    private boolean requiresFilterExp(Qualifier qualifier) {
        if (!qualifier.hasSecIndexFilter()) {
            return true;
        } else if (qualifier.hasSecIndexFilter() && dualFilterOperations.contains(qualifier.getOperation())) {
            qualifier.setHasSecIndexFilter(false); // clear the flag in case if the same Qualifier is going to be reused
            return true;
        } else {
            qualifier.setHasSecIndexFilter(false); // clear the flag in case if the same Qualifier is going to be reused
            return false;
        }
    }
}
