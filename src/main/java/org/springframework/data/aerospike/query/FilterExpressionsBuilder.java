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
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.aerospike.query.qualifier.Qualifier;

@Slf4j
public class FilterExpressionsBuilder {

    public Expression build(Qualifier qualifier) {
        if (qualifier != null && !qualifier.isEmpty()) {
            Exp exp = qualifier.getFilterExp();
            if (exp == null) {
                log.debug("Query #{}, filterExp is not set", qualifier.hashCode());
                return null;
            }
            log.debug("Query #{}, filterExp is set", qualifier.hashCode());
            return Exp.build(exp);
        }
        return null;
    }
}
