/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.aerospike;

import org.springframework.dao.InvalidDataAccessResourceUsageException;

/**
 * @deprecated since Aerospike Server ver. 6.1.0.1. Creating secondary index no longer throws an exception if the index
 * already exists.
 * <p>Use {@link org.springframework.data.aerospike.repository.AerospikeRepository#indexExists(String)}
 * for secondary index existence checks.</p>
 */
@Deprecated
public class IndexAlreadyExistsException extends InvalidDataAccessResourceUsageException {

    public IndexAlreadyExistsException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
