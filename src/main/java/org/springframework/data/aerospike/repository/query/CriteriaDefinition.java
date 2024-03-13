/*
 * Copyright 2015 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.repository.query;

import com.aerospike.client.query.Filter;
import org.springframework.data.aerospike.query.qualifier.Qualifier;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public interface CriteriaDefinition {

    /**
     * Get {@link Filter} representation.
     */
    Qualifier getCriteriaObject();

    /**
     * Get the identifying {@literal field}.
     *
     * @since 1.6
     */
    String getCriteriaField();

    enum AerospikeQueryCriterion {
        KEY, VALUE, KEY_VALUE_PAIR
    }

    enum AerospikeNullQueryCriterion {
        NULL_PARAM
    }

    enum AerospikeMetadata {
        SINCE_UPDATE_TIME, LAST_UPDATE_TIME, VOID_TIME, TTL, RECORD_SIZE_ON_DISK, RECORD_SIZE_IN_MEMORY
    }
}
